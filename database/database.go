package database

import  (
	"context"
	"database/sql"
	"github.com/denismitr/tern/internal/logger"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
	"io"
)

var ErrUnsupportedDBDriver = errors.New("unknown DB driver")
var ErrNothingToMigrate = errors.New("nothing to migrate")
var ErrMigrationVersionNotSpecified = errors.New("migration version not specified")

const (
	DefaultMigrationsTable = "migrations"

	operationRollback = "rollback"
	operationMigrate  = "migrate"
	operationRefresh  = "refresh"
)

type CommonOptions struct {
	MigrationsTable string
}

type ctxExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type Plan struct {
	Steps int
}

type ServiceGateway interface {
	io.Closer

	WriteVersions(ctx context.Context, migrations migration.Migrations) error
	ReadVersions(ctx context.Context) ([]migration.Version, error)
	ShowTables(ctx context.Context) ([]string, error)
	DropMigrationsTable(ctx context.Context) error
	CreateMigrationsTable(ctx context.Context) error
}

type Gateway interface {
	io.Closer

	SetLogger(logger.Logger)

	Migrate(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Rollback(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error)
	Refresh(ctx context.Context, migrations migration.Migrations, plan Plan) (migration.Migrations, migration.Migrations, error)
}

// CreateServiceGateway - creates gateway with service functionality
// such as listing all tables in database and reading migration versions
func CreateServiceGateway(driver string, db *sql.DB, migrationsTable string) (ServiceGateway, error) {
	connector := MakeRetryingConnector(db, NewDefaultConnectOptions())

	switch driver {
	case "mysql":
		return NewMySQLGateway(connector,
			&MySQLOptions{
				CommonOptions: CommonOptions{
					MigrationsTable: migrationsTable,
				},
				LockFor: MysqlDefaultLockSeconds,
				LockKey: MysqlDefaultLockKey,
			})
	case "sqlite3", "sqlite":
		return NewSqliteGateway(
			connector,
			&SqliteOptions{
				CommonOptions{
					MigrationsTable: migrationsTable,
				},
			},
		)
	}

	return nil, errors.Wrapf(ErrUnsupportedDBDriver, "%s is not supported by Tern library", driver)
}

type queryBuilder interface {
	createMigrationsSchemaQuery() string
	createInsertVersionsQuery() string
	createDeleteVersionQuery() string
	createShowTablesQuery() string
	createDropMigrationsSchemaQuery() string
}

type dbh struct {
	locker          locker
	lg              logger.Logger
	connector       connector
	conn            *sql.Conn
	migrationsTable string
	qb              queryBuilder
}

func newDbh(connector connector, lg logger.Logger, locker locker, qb queryBuilder, table string) *dbh {
	return &dbh{
		connector: connector,
		lg: lg,
		locker: locker,
		migrationsTable: table,
		qb: qb,
	}
}

func (db *dbh) connect(ctx context.Context) error {
	if db.conn == nil {
		if conn, err := db.connector.connect(ctx); err != nil {
			return err
		} else {
			db.conn = conn
		}
	}

	return nil
}

func (db *dbh) execUnderLock(ctx context.Context, operation string, f func(*sql.Tx, []migration.Version) error) error {
	if err := db.connect(ctx); err != nil {
		return err
	}

	if err := db.locker.lock(ctx, db.conn); err != nil {
		return errors.Wrap(err, "database lock failed")
	}

	handleError := func(err error, tx *sql.Tx) error {
		var rollbackErr error
		var unlockErr error
		var result = err

		if tx != nil {
			rollbackErr = tx.Rollback()
			if rollbackErr != nil {
				result = errors.Wrapf(result, rollbackErr.Error())
			}
		}

		unlockErr = db.locker.unlock(ctx, db.conn)
		if unlockErr != nil {
			result = errors.Wrapf(result, unlockErr.Error())
		}

		return result
	}

	if err := db.createMigrationsTable(ctx); err != nil {
		return handleError(err, nil)
	}

	tx, err := db.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return handleError(errors.Wrapf(err, "could not start transaction to execute [%s] operation", operation), nil)
	}

	availableVersions, err := readVersions(tx, db.migrationsTable)
	if err != nil {
		return handleError(errors.Wrapf(err, "operation [%s] failed", operation), tx)
	}

	if err := f(tx, availableVersions); err != nil {
		return handleError(errors.Wrapf(err, "operation [%s] failed", operation), tx)
	}

	if err := tx.Commit(); err != nil {
		return handleError(errors.Wrapf(err, "could not commit [%s] operation, rolled back", operation), tx)
	}

	return db.locker.unlock(ctx, db.conn)
}

func (db *dbh) createMigrationsTable(ctx context.Context) error {
	if _, err := db.conn.ExecContext(ctx, db.qb.createMigrationsSchemaQuery()); err != nil {
		return err
	}

	return nil
}

func (db *dbh) migrateAll(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	var migrated migration.Migrations

	if err := db.execUnderLock(ctx, operationMigrate, func(tx *sql.Tx, versions []migration.Version) error {
		insertVersionQuery := db.qb.createInsertVersionsQuery()

		var scheduled migration.Migrations
		for i := range migrations {
			if !inVersions(migrations[i].Version, versions) {
				if p.Steps != 0 && len(scheduled) >= p.Steps {
					break
				}

				scheduled = append(scheduled, migrations[i])
			}
		}

		if len(scheduled) == 0 {
			return ErrNothingToMigrate
		}

		for i := range scheduled {
			if err := db.migrateOne(ctx, tx, scheduled[i], insertVersionQuery); err != nil {
				return err
			}

			db.lg.Successf("migrated: %s", scheduled[i].Key)

			migrated = append(migrated, scheduled[i])
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return migrated, nil
}

func (db *dbh) migrateOne(ctx context.Context, tx ctxExecutor, migration *migration.Migration, insertQuery string) error {
	if migration.Version.Value == "" {
		return ErrMigrationVersionNotSpecified
	}

	db.lg.SQL(migration.MigrateScripts())

	if _, err := tx.ExecContext(ctx, migration.MigrateScripts()); err != nil {
		return errors.Wrapf(err, "could not run migration [%s]", migration.Key)
	}

	db.lg.SQL(insertQuery, migration.Version.Value, migration.Name)

	if _, err := tx.ExecContext(ctx, insertQuery, migration.Version.Value, migration.Name); err != nil {
		return errors.Wrapf(
			err,
			"could not insert migration version [%s]",
			migration.Version.Value,
		)
	}

	return nil
}

func (db *dbh) rollbackAll(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	var executed migration.Migrations

	if err := db.execUnderLock(ctx, operationRollback, func(tx *sql.Tx, versions []migration.Version) error {
		deleteVersionQuery := db.qb.createDeleteVersionQuery()

		var scheduled migration.Migrations
		for i := len(migrations) - 1; i >= 0; i-- {
			if inVersions(migrations[i].Version, versions) {
				if p.Steps != 0 && len(scheduled) >= p.Steps {
					break
				}

				scheduled = append(scheduled, migrations[i])
			}
		}

		if len(scheduled) == 0 {
			return ErrNothingToMigrate
		}

		for i := range scheduled {
			db.lg.Debugf("rolling back: %s", scheduled[i].Key)
			if err := db.rollbackOne(ctx, tx, scheduled[i], deleteVersionQuery); err != nil {
				return err
			}

			db.lg.Successf("rolled back: %s", scheduled[i].Key)

			executed = append(executed, scheduled[i])
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return executed, nil
}

func (db *dbh) rollbackOne(ctx context.Context, ex ctxExecutor, migration *migration.Migration, removeVersionQuery string) error {
	if migration.Version.Value == "" {
		return ErrMigrationVersionNotSpecified
	}

	if migration.RollbackScripts() != "" {
		db.lg.SQL(migration.RollbackScripts())

		if _, err := ex.ExecContext(ctx, migration.RollbackScripts()); err != nil {
			return errors.Wrapf(err, "could not rollback migration [%s]", migration.Key)
		}
	}

	db.lg.SQL(removeVersionQuery, migration.Version.Value)

	if _, err := ex.ExecContext(ctx, removeVersionQuery, migration.Version.Value); err != nil {
		return errors.Wrapf(
			err,
			"could not remove migration version [%s]",
			migration.Version.Value,
		)
	}

	return nil
}

func (db *dbh) refresh(
	ctx context.Context,
	migrations migration.Migrations,
	plan Plan,
) (migration.Migrations, migration.Migrations, error) {
	var rolledBack migration.Migrations
	var migrated migration.Migrations

	if err := db.execUnderLock(ctx, operationRefresh, func(tx *sql.Tx, versions []migration.Version) error {
		deleteVersionQuery := db.qb.createDeleteVersionQuery()
		insertVersionQuery := db.qb.createInsertVersionsQuery()

		for i := len(migrations) - 1; i >= 0; i-- {
			if inVersions(migrations[i].Version, versions) {
				if err := db.rollbackOne(ctx, tx, migrations[i], deleteVersionQuery); err != nil {
					return err
				}

				rolledBack = append(rolledBack, migrations[i])
			}
		}

		for i := range migrations {
			if err := db.migrateOne(ctx, tx, migrations[i], insertVersionQuery); err != nil {
				return err
			}

			migrated = append(migrated, migrations[i])
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}

	return rolledBack, migrated, nil
}

func (db *dbh) readVersions(ctx context.Context) ([]migration.Version, error) {
	if err := db.connect(ctx); err != nil {
		return nil, err
	}

	tx, err := db.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	result, err := readVersions(tx, db.migrationsTable)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return result, err
	}

	return result, nil
}

func (db *dbh) showTables(ctx context.Context) ([]string, error) {
	if err := db.connect(ctx); err != nil {
		return nil, err
	}

	rows, err := db.conn.QueryContext(ctx, db.qb.createShowTablesQuery())
	if err != nil {
		return nil, errors.Wrap(err, "could not list all tables")
	}

	var result []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = rows.Close()
			return result, err
		}
		result = append(result, table)
	}

	return result, err
}

func (db *dbh) writeVersions(ctx context.Context, migrations migration.Migrations) error {
	if err := db.connect(ctx); err != nil {
		return err
	}

	query := db.qb.createInsertVersionsQuery()

	tx, err := db.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	for i := range migrations {
		name := migrations[i].Name
		timestamp := migrations[i].Version.Value
		if _, err := db.conn.ExecContext(ctx, query, timestamp, name); err != nil {
			_ = tx.Rollback() // fixme
			return errors.Wrapf(err, "could not insert migration with version [%s] and name [%s] to [%s] table", timestamp, name, db.migrationsTable)
		}
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback() // fixme
	}

	return nil
}

func (db *dbh) close() error {
	if db.conn != nil {
		return db.conn.Close()
	}

	return nil // fixme
}

func (db *dbh) dropMigrationsTable(ctx context.Context) error {
	if err := db.connect(ctx); err != nil {
		return err
	}

	if _, err := db.conn.ExecContext(ctx, db.qb.createDropMigrationsSchemaQuery()); err != nil {
		return err
	}

	return nil
}

func (db *dbh) setLogger(lg logger.Logger) {
	db.lg = lg
}
