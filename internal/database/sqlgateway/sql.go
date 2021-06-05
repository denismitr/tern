package sqlgateway

import (
	"context"
	"database/sql"
	"github.com/denismitr/tern/v2/internal/database"
	"github.com/denismitr/tern/v2/internal/logger"
	"github.com/denismitr/tern/v2/migration"
	"github.com/pkg/errors"
	"time"
)

const MysqlDefaultLockKey = "tern_migrations"
const MysqlDefaultLockSeconds = 3

type ctxExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type MySQLOptions struct {
	database.CommonOptions
	LockKey string
	LockFor int // maybe refactor to duration
	NoLock  bool
}

type SQLGateway struct {
	locker    locker
	lg        logger.Logger
	conn      *sql.Conn
	connector SQLConnector
	schema    schema
}

var _ database.Gateway = (*SQLGateway)(nil)

// NewMySQLGateway - creates a new MySQL gateway and uses the SQLConnector interface to attempt to
// Connect to the MySQL database
func NewMySQLGateway(connector SQLConnector, options *MySQLOptions) (*SQLGateway, database.ConnCloser) {
	gateway := SQLGateway{}
	gateway.connector = connector
	gateway.locker = newMySQLLocker(options.LockKey, options.LockFor, options.NoLock)

	if options.MigrationsTable == "" {
		options.MigrationsTable = database.DefaultMigrationsTable
	}

	if options.MigratedAtColumn == "" {
		options.MigratedAtColumn = database.MigratedAtColumn
	}

	gateway.schema = newMysqlSchemaV1(options.MigrationsTable, options.MigratedAtColumn, "utf8")

	return &gateway, connector.Close
}

// NewSqliteGateway - creates a new SQL gateway
func NewSqliteGateway(connector SQLConnector, options *SqliteOptions) (*SQLGateway, database.ConnCloser) {
	gateway := SQLGateway{}
	gateway.connector = connector
	gateway.locker = &nullLocker{}

	if options.MigrationsTable == "" {
		options.MigrationsTable = database.DefaultMigrationsTable
	}

	if options.MigratedAtColumn == "" {
		options.MigratedAtColumn = database.MigratedAtColumn
	}

	gateway.schema = newSqliteSchemaV1(options.MigrationsTable, options.MigratedAtColumn)

	return &gateway, connector.Close
}

func (g *SQLGateway) SetLogger(lg logger.Logger) {
	g.lg = lg
}

func (g *SQLGateway) Connect() error {
	if g.conn != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.connector.Timeout())
	defer cancel()

	conn, err := g.connector.Connect(ctx)
	if err != nil {
		return err
	}

	g.conn = conn
	return nil
}

func (g *SQLGateway) Migrate(ctx context.Context, migrations migration.Migrations, p database.Plan) (migration.Migrations, error) {
	var migrated migration.Migrations

	if err := g.execUnderLock(ctx, database.OperationMigrate, func(tx *sql.Tx, migratedVersions []migration.Version) error {
		scheduled := database.ScheduleForMigration(migrations, migratedVersions, p)

		if len(scheduled) == 0 {
			return database.ErrNoChangesRequired
		}

		for i := range scheduled {
			if err := g.migrateOne(ctx, tx, scheduled[i]); err != nil {
				return err
			}

			g.lg.Successf("migrated: %s", scheduled[i].Key)

			migrated = append(migrated, scheduled[i])
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return migrated, nil
}

func (g *SQLGateway) Rollback(ctx context.Context, migrations migration.Migrations, p database.Plan) (migration.Migrations, error) {
	var executed migration.Migrations

	if err := g.execUnderLock(ctx, database.OperationRollback, func(tx *sql.Tx, migratedVersions []migration.Version) error {
		scheduled := database.ScheduleForRollback(migrations, migratedVersions, p)

		if len(scheduled) == 0 {
			return database.ErrNoChangesRequired
		}

		for i := range scheduled {
			g.lg.Debugf("rolling back: %s", scheduled[i].Key)
			if err := g.rollbackOne(ctx, tx, scheduled[i]); err != nil {
				return err
			}

			g.lg.Successf("rolled back: %s", scheduled[i].Key)

			executed = append(executed, scheduled[i])
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return executed, nil
}

func (g *SQLGateway) Refresh(
	ctx context.Context,
	migrations migration.Migrations,
	p database.Plan,
) (migration.Migrations, migration.Migrations, error) {
	var rolledBack migration.Migrations
	var migrated migration.Migrations

	if err := g.execUnderLock(ctx, database.OperationRefresh, func(tx *sql.Tx, migratedVersions []migration.Version) error {
		scheduled := database.ScheduleForRefresh(migrations, migratedVersions, p)

		if len(scheduled) == 0 {
			return database.ErrNoChangesRequired
		}

		for i := range scheduled {
			g.lg.Debugf("rolling back: %s", scheduled[i].Key)
			if err := g.rollbackOne(ctx, tx, scheduled[i]); err != nil {
				return err
			}

			rolledBack = append(rolledBack, scheduled[i])
			g.lg.Successf("rolled back: %s", scheduled[i].Key)
		}

		for i := len(scheduled) - 1; i >= 0; i-- {
			g.lg.Debugf("migrating: %s", scheduled[i].Key)
			if err := g.migrateOne(ctx, tx, scheduled[i]); err != nil {
				return err
			}

			migrated = append(migrated, scheduled[i])
			g.lg.Debugf("migrated: %s", scheduled[i].Key)
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}

	return rolledBack, migrated, nil
}

func (g *SQLGateway) ReadVersions(ctx context.Context) ([]migration.Version, error) {
	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	result, err := g.readVersionsUnderTx(tx, readVersionsFilter{Sort: ASC})
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return result, err
	}

	return result, nil
}

func (g *SQLGateway) CreateMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, g.schema.initQuery()); err != nil {
		return err
	}

	return nil
}

func (g *SQLGateway) DropMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, g.schema.dropQuery()); err != nil {
		return err
	}

	return nil
}

func (g *SQLGateway) WriteVersions(ctx context.Context, migrations migration.Migrations) error {
	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrap(err, "could not start transaction to write migratrions")
	}

	for _, m := range migrations {
		insertQuery, args := g.schema.insertQuery(m)
		if _, err := g.conn.ExecContext(ctx, insertQuery, args...); err != nil {
			execErr := errors.Wrapf(err, "could not insert migration with query %s and args %+v", insertQuery, args)
			if errRb := tx.Rollback(); errRb != nil {
				return errors.Wrapf(errRb, "could not rollback write migration versions transaction after error %s", execErr)
			}

			return execErr
		}
	}

	if err := tx.Commit(); err != nil {
		if errRb := tx.Rollback(); errRb != nil {
			return errors.Wrap(errRb, "could not rollback write migration versions transaction")
		}
	}

	return nil
}

func (g *SQLGateway) ShowTables(ctx context.Context) ([]string, error) {
	rows, err := g.conn.QueryContext(ctx, g.schema.showTablesQuery())
	if err != nil {
		return nil, errors.Wrap(err, "could not list all tables")
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, errors.Wrap(err, "show tables rows error")
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			g.lg.Error(closeErr)
		}
	}()

	var result []string
	for rows.Next() {
		if errRows := rows.Err(); errRows != nil {
			if errRows != sql.ErrNoRows {
				return nil, errors.Wrap(errRows, "migration table iteration error")
			} else {
				break
			}
		}

		var table string
		if errScan := rows.Scan(&table); errScan != nil {
			return result, errScan
		}

		result = append(result, table)
	}

	return result, err
}

func (g *SQLGateway) execUnderLock(ctx context.Context, operation string, f func(*sql.Tx, []migration.Version) error) error {
	if err := g.locker.lock(ctx, g.conn); err != nil {
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

		unlockErr = g.locker.unlock(ctx, g.conn)
		if unlockErr != nil {
			result = errors.Wrapf(result, unlockErr.Error())
		}

		return result
	}

	if err := g.CreateMigrationsTable(ctx); err != nil {
		return handleError(err, nil)
	}

	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return handleError(errors.Wrapf(err, "could not start transaction to execute [%s] operation", operation), nil)
	}

	availableVersions, err := g.readVersionsUnderTx(tx, readVersionsFilter{Sort: ASC})
	if err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			return handleError(err, tx)
		}

		return handleError(errors.Wrapf(err, "operation [%s] failed", operation), tx)
	}

	if err := f(tx, availableVersions); err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			return handleError(err, tx)
		}

		return handleError(errors.Wrapf(err, "operation [%s] failed", operation), tx)
	}

	if err := tx.Commit(); err != nil {
		return handleError(errors.Wrapf(err, "could not commit [%s] operation, rolled back", operation), tx)
	}

	return g.locker.unlock(ctx, g.conn)
}

func (g *SQLGateway) migrateOne(ctx context.Context, ex ctxExecutor, m *migration.Migration) error {
	if m.Version.Value == "" {
		return database.ErrMigrationVersionNotSpecified
	}

	insertQuery, args := g.schema.insertQuery(m)

	if len(m.Migrate) > 0 {
		for _, script := range m.Migrate {
			g.lg.SQL(script)
			if _, err := ex.ExecContext(ctx, script); err != nil {
				return errors.Wrapf(err, "could not migrate script [%s], migration [%s]", script, m.Key)
			}
		}
	}

	g.lg.SQL(insertQuery, m.Version.Value, m.Name)

	if _, err := ex.ExecContext(ctx, insertQuery, args...); err != nil {
		return errors.Wrapf(
			err,
			"could not insert migration version [%s]",
			m.Version.Value,
		)
	}

	return nil
}

func (g *SQLGateway) rollbackOne(ctx context.Context, ex ctxExecutor, m *migration.Migration) error {
	if m.Version.Value == "" {
		return database.ErrMigrationVersionNotSpecified
	}

	removeVersionQuery, args := g.schema.removeQuery(m)

	if len(m.Rollback) > 0 {
		for _, script := range m.Rollback {
			g.lg.SQL(script)
			if _, err := ex.ExecContext(ctx, script); err != nil {
				return errors.Wrapf(err, "could not rollback script [%s], migration [%s]", script, m.Key)
			}
		}
	}

	g.lg.SQL(removeVersionQuery, m.Version.Value)

	if _, err := ex.ExecContext(ctx, removeVersionQuery, args...); err != nil {
		return errors.Wrapf(
			err,
			"could not remove migration version [%s]",
			m.Version.Value,
		)
	}

	return nil
}

func (g *SQLGateway) readVersionsUnderTx(tx *sql.Tx, f readVersionsFilter) ([]migration.Version, error) {
	q := g.schema.readVersionsQuery(f)
	rows, err := tx.Query(q)
	if err != nil {
		return nil, errors.Wrap(err, "could not execute query")
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, errors.Wrap(err, "read versions error")
	}

	var result []migration.Version

	defer func() {
		if err := rows.Close(); err != nil {
			g.lg.Error(err)
		}
	}()

	for rows.Next() {
		if errRows := rows.Err(); errRows != nil {
			if errRows != sql.ErrNoRows {
				return nil, errors.Wrap(errRows, "read migration versions iteration failed")
			} else {
				break
			}
		}

		var timestamp string
		var migratedAt time.Time
		if err := rows.Scan(&timestamp, &migratedAt); err != nil {
			if closeErr := rows.Close(); closeErr != nil {
				g.lg.Error(closeErr)
			}

			return result, err
		}

		result = append(result, migration.Version{Value: timestamp, MigratedAt: migratedAt})
	}

	return result, nil
}
