package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/denismitr/tern/logger"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
)

const (
	mysqlCreateMigrationsSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(13) PRIMARY KEY,
			name VARCHAR(255),
			created_at TIMESTAMP default CURRENT_TIMESTAMP
		) ENGINE=INNODB;	
	`
	mysqlDeleteVersionQuery = "DELETE FROM %s WHERE version = ?;"
	MysqlDropMigrationsSchema = `DROP TABLE IF EXISTS %s;`
	mysqlInsertVersionQuery = "INSERT INTO %s (version, name) VALUES (?, ?);"
)


const MysqlDefaultLockKey = "tern_migrations"
const MysqlDefaultLockSeconds = 3

type MySQLOptions struct {
	CommonOptions
	LockKey string
	LockFor int
}

type mySQLLocker struct {
	lockKey string
	lockFor int
}

func (g *mySQLLocker) lock(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "SELECT GET_LOCK(?, ?)", g.lockKey, g.lockFor); err != nil {
		return errors.Wrapf(err, "could not obtain [%s] exclusive MySQL DB lock for [%d] seconds", g.lockKey, g.lockFor)
	}

	return nil
}

func (g *mySQLLocker) unlock(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", g.lockKey); err != nil {
		return errors.Wrapf(err, "could not release [%s] exclusive MySQL DB lock", g.lockKey)
	}

	return nil
}

type MySQLGateway struct {
	handlers

	db              *sql.DB
	conn            *sql.Conn
	lg              logger.Logger
	migrationsTable string
	locker          locker
}

var _ Gateway = (*MySQLGateway)(nil)
var _ ServiceGateway = (*MySQLGateway)(nil)

// NewMySQLGateway - creates a new MySQL gateway and uses the connector interface to attempt to
// connect to the MySQL database
func NewMySQLGateway(db *sql.DB, connector connector, options *MySQLOptions) (*MySQLGateway, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connector.timeout())
	defer cancel()

	conn, err := connector.connect(ctx, db)
	if err != nil {
		return nil, err
	}

	return &MySQLGateway{
		db:   db,
		conn: conn,
		handlers: handlers{
			migrate:  migrate,
			rollback: rollback,
		},
		lg: &logger.NullLogger{},
		migrationsTable: options.MigrationsTable,
		locker: &mySQLLocker{
			lockKey: options.LockKey,
			lockFor: options.LockFor,
		},
	}, nil
}

// Close the connection
func (g *MySQLGateway) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}

	return nil
}

func (g *MySQLGateway) SetLogger(lg logger.Logger) {
	g.lg = lg
}

func (g *MySQLGateway) Migrate(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	var migrated migration.Migrations

	if err := g.execUnderLock(ctx, operationMigrate, func(tx *sql.Tx, versions []migration.Version) error {
		insertVersionQuery := g.createInsertVersionsQuery()

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
			if err := g.migrate(ctx, tx, g.lg, scheduled[i], insertVersionQuery); err != nil {
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

func (g *MySQLGateway) Rollback(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	var executed migration.Migrations

	if err := g.execUnderLock(ctx, operationRollback, func(tx *sql.Tx, versions []migration.Version) error {
		deleteVersionQuery := g.createDeleteVersionQuery()

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
			g.lg.Debugf("rolling back: %s", scheduled[i].Key)
			if err := g.rollback(ctx, tx, g.lg, scheduled[i], deleteVersionQuery); err != nil {
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

func (g *MySQLGateway) Refresh(
	ctx context.Context,
	migrations migration.Migrations,
	plan Plan,
) (migration.Migrations, migration.Migrations, error) {
	var rolledBack migration.Migrations
	var migrated migration.Migrations

	if err := g.execUnderLock(ctx, operationRefresh, func(tx *sql.Tx, versions []migration.Version) error {
		deleteVersionQuery := g.createDeleteVersionQuery()
		insertVersionQuery := g.createInsertVersionsQuery()

		for i := len(migrations) - 1; i >= 0; i-- {
			if inVersions(migrations[i].Version, versions) {
				if err := g.rollback(ctx, tx, g.lg, migrations[i], deleteVersionQuery); err != nil {
					return err
				}

				rolledBack = append(rolledBack, migrations[i])
			}
		}

		for i := range migrations {
			if err := g.migrate(ctx, tx, g.lg, migrations[i], insertVersionQuery); err != nil {
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

func (g *MySQLGateway) ReadVersions(ctx context.Context) ([]migration.Version, error) {
	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	result, err := readVersions(tx, g.migrationsTable)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return result, err
	}

	return result, nil
}

func (g *MySQLGateway) createDeleteVersionQuery() string {
	return fmt.Sprintf(mysqlDeleteVersionQuery, g.migrationsTable)
}

func (g *MySQLGateway) createInsertVersionsQuery() string {
	return fmt.Sprintf(mysqlInsertVersionQuery, g.migrationsTable)
}

func (g *MySQLGateway) CreateMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, fmt.Sprintf(mysqlCreateMigrationsSchema, g.migrationsTable)); err != nil {
		return err
	}

	return nil
}

func (g *MySQLGateway) DropMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, fmt.Sprintf(MysqlDropMigrationsSchema, g.migrationsTable)); err != nil {
		return err
	}

	return nil
}

func (g *MySQLGateway) WriteVersions(ctx context.Context, migrations migration.Migrations) error {
	query := g.createInsertVersionsQuery()

	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	for i := range migrations {
		name := migrations[i].Name
		timestamp := migrations[i].Version.Timestamp
		if _, err := g.conn.ExecContext(ctx, query, timestamp, name); err != nil {
			_ = tx.Rollback()
			return errors.Wrapf(err, "could not insert migration with version [%s] and name [%s] to [%s] table", timestamp, name, g.migrationsTable)
		}
	}

	return tx.Commit()
}

func (g *MySQLGateway) ShowTables(ctx context.Context) ([]string, error) {
	rows, err := g.conn.QueryContext(ctx, "SHOW TABLES;")
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

func (g *MySQLGateway) execUnderLock(ctx context.Context, operation string, f func(*sql.Tx, []migration.Version) error) error {
	if err := g.locker.lock(ctx, g.conn); err != nil {
		return errors.Wrap(err, "mysql lock failed")
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

	availableVersions, err := readVersions(tx, g.migrationsTable)
	if err != nil {
		return handleError(errors.Wrapf(err, "operation [%s] failed", operation), tx)
	}

	if err := f(tx, availableVersions); err != nil {
		return handleError(errors.Wrapf(err, "operation [%s] failed", operation), tx)
	}

	if err := tx.Commit(); err != nil {
		return handleError(errors.Wrapf(err, "could not commit [%s] operation, rolled back", operation), tx)
	}

	return g.locker.unlock(ctx, g.conn)
}
