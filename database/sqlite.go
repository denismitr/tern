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
	sqliteCreateMigrationsSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(13) PRIMARY KEY,
			name VARCHAR(255),
			created_at TIMESTAMP default CURRENT_TIMESTAMP
		);	
	`
	sqliteDeleteVersionQuery = "DELETE FROM %s WHERE version = ?;"
	sqliteDropMigrationsSchema = "DROP TABLE IF EXISTS %s;"
	sqliteInsertVersionQuery = "INSERT INTO %s (version, name) VALUES (?, ?);"
	sqliteShowTablesQuery = "SHOW TABLES;"
)

type SqliteOptions struct {
	CommonOptions
}

type SqliteGateway struct {
	handlers

	db              *sql.DB
	conn            *sql.Conn
	lg              logger.Logger
	migrationsTable string
}

var _ Gateway = (*SqliteGateway)(nil)
var _ ServiceGateway = (*SqliteGateway)(nil)

// NewSqliteGateway - creates a new Sqlite gateway and uses the connector interface to attempt to
// connect to the sqlite database
func NewSqliteGateway(db *sql.DB, connector connector, options *SqliteOptions) (*SqliteGateway, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connector.timeout())
	defer cancel()

	conn, err := connector.connect(ctx, db)
	if err != nil {
		return nil, err
	}

	return &SqliteGateway{
		db:   db,
		conn: conn,
		handlers: handlers{
			migrate:  migrate,
			rollback: rollback,
		},
		lg: &logger.NullLogger{},
		migrationsTable: options.MigrationsTable,
	}, nil
}

func (g *SqliteGateway) WriteVersions(ctx context.Context, migrations migration.Migrations) error {
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

func (g *SqliteGateway) ReadVersions(ctx context.Context) ([]migration.Version, error) {
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

func (g *SqliteGateway) ShowTables(ctx context.Context) ([]string, error) {
	rows, err := g.conn.QueryContext(ctx, sqliteShowTablesQuery)
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

func (g *SqliteGateway) DropMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, fmt.Sprintf(sqliteDropMigrationsSchema, g.migrationsTable)); err != nil {
		return err
	}

	return nil
}

func (g *SqliteGateway) CreateMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, fmt.Sprintf(sqliteCreateMigrationsSchema, g.migrationsTable)); err != nil {
		return err
	}

	return nil
}

func (g *SqliteGateway) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}

	return nil
}

func (g *SqliteGateway) SetLogger(lg logger.Logger) {
	g.lg = lg
}

func (g *SqliteGateway) Migrate(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
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

func (g *SqliteGateway) Rollback(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
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

func (g *SqliteGateway) Refresh(ctx context.Context, migrations migration.Migrations, plan Plan) (migration.Migrations, migration.Migrations, error) {
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

func (g *SqliteGateway) execUnderLock(ctx context.Context, operation string, f func(*sql.Tx, []migration.Version) error) error {
	//if err := g.locker.lock(ctx, g.conn); err != nil {
	//	return errors.Wrap(err, "mysql lock failed")
	//}

	handleError := func(err error, tx *sql.Tx) error {
		var rollbackErr error
		//var unlockErr error
		var result = err

		if tx != nil {
			rollbackErr = tx.Rollback()
			if rollbackErr != nil {
				result = errors.Wrapf(result, rollbackErr.Error())
			}
		}

		//unlockErr = g.locker.unlock(ctx, g.conn)
		//if unlockErr != nil {
		//	result = errors.Wrapf(result, unlockErr.Error())
		//}

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

	//return g.locker.unlock(ctx, g.conn)
	return nil
}

func (g *SqliteGateway) createInsertVersionsQuery() string {
	return fmt.Sprintf(sqliteInsertVersionQuery, g.migrationsTable)
}

func (g *SqliteGateway) createDeleteVersionQuery() string {
	return fmt.Sprintf(sqliteDeleteVersionQuery, g.migrationsTable)
}