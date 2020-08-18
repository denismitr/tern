package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/denismitr/tern/migration"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const mysqlCreateMigrationsSchema = `
CREATE TABLE IF NOT EXISTS %s (
	version VARCHAR(13) PRIMARY KEY,
	name VARCHAR(255),
	created_at TIMESTAMP default CURRENT_TIMESTAMP
) ENGINE=INNODB;	
`

const DefaultMigrationsTable = "migrations"
const MysqlDropMigrationsSchema = `DROP TABLE IF EXISTS %s;`
const MysqlDefaultLockKey = "tern_migrations"
const MysqlDefaultLockSeconds = 3

type Plan struct {
	Steps int
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

type MySQL struct {
	db              *sqlx.DB
	conn            *sql.Conn
	migrationsTable string
	locker          locker
}

// Close the connection
func (g *MySQL) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}

	return nil
}

func (g *MySQL) Up(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	var migrated migration.Migrations

	if err := g.execUnderLock(ctx, "migrate", func(tx *sql.Tx, versions []string) error {
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
			if err := up(ctx, tx, scheduled[i], insertVersionQuery); err != nil {
				return err
			}

			migrated = append(migrated, scheduled[i])
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return migrated, nil
}

func (g *MySQL) Down(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	var executed migration.Migrations

	if err := g.execUnderLock(ctx, "rollback", func(tx *sql.Tx, versions []string) error {
		deleteVersionQuery := g.createDeleteVersionQuery()

		for i := range migrations {
			if inVersions(migrations[i].Version, versions) {
				if err := down(ctx, tx, migrations[i], deleteVersionQuery); err != nil {
					return err
				}

				executed = append(executed, migrations[i])
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return executed, nil
}

func (g *MySQL) Refresh(
	ctx context.Context,
	migrations migration.Migrations,
	plan Plan,
) (migration.Migrations, migration.Migrations, error) {
	var rolledBack migration.Migrations
	var migrated migration.Migrations

	if err := g.execUnderLock(ctx, "refresh", func(tx *sql.Tx, versions []string) error {
		deleteVersionQuery := g.createDeleteVersionQuery()
		insertVersionQuery := g.createInsertVersionsQuery()

		for i := range migrations {
			if inVersions(migrations[i].Version, versions) {
				if err := down(ctx, tx, migrations[i], deleteVersionQuery); err != nil {
					return err
				}

				rolledBack = append(rolledBack, migrations[i])
			}
		}

		for i := range migrations {
			if err := up(ctx, tx, migrations[i], insertVersionQuery); err != nil {
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

func (g *MySQL) execUnderLock(ctx context.Context, operation string, f func(*sql.Tx, []string) error) error {
	if err := g.locker.lock(ctx, g.conn); err != nil {
		return errors.Wrap(err, "down migrations lock failed")
	}

	defer func() {
		if err := g.locker.unlock(ctx, g.conn); err != nil {
			panic(err) // fixme
		}
	}()

	if err := g.CreateMigrationsTable(ctx); err != nil {
		return err
	}

	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrapf(err, "could not start transaction to execute [%s] operation", operation)
	}

	availableVersions, err := readVersions(tx, g.migrationsTable)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return errors.Wrapf(err, "operation [%s] failed: %s", operation, rollbackErr.Error())
		}
		return err
	}

	if err := f(tx, availableVersions); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return errors.Wrapf(err, "operation [%s] failed: %s", operation, rollbackErr.Error())
		}

		return errors.Wrapf(err, "operation [%s] failed", operation)
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "could not commit [%s] operation]", operation)
	}

	return nil
}

func (g *MySQL) ReadVersions(ctx context.Context) ([]string, error) {
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

func NewMysqlGateway(db *sqlx.DB, tableName, lockKey string, lockFor int) (*MySQL, error) {
	conn, err := db.Conn(context.Background()) // fixme
	if err != nil {
		return nil, errors.Wrap(err, "could not establish DB connection")
	}

	return &MySQL{
		db:              db,
		conn:            conn,
		migrationsTable: tableName,
		locker: &mySQLLocker{
			lockKey: lockKey,
			lockFor: lockFor,
		},
	}, nil
}

func (g *MySQL) createDeleteVersionQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE version = ?;", g.migrationsTable)
}

func (g *MySQL) createInsertVersionsQuery() string {
	return fmt.Sprintf("INSERT INTO %s (version, name) VALUES (?, ?);", g.migrationsTable)
}

func (g *MySQL) CreateMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, fmt.Sprintf(mysqlCreateMigrationsSchema, g.migrationsTable)); err != nil {
		return err
	}

	return nil
}

func (g *MySQL) DropMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, fmt.Sprintf(MysqlDropMigrationsSchema, g.migrationsTable)); err != nil {
		return err
	}

	return nil
}

func (g *MySQL) WriteVersions(ctx context.Context, migrations migration.Migrations) error {
	query := g.createInsertVersionsQuery()

	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	for i := range migrations {
		name := migrations[i].Name
		version := migrations[i].Version
		if _, err := g.conn.ExecContext(ctx, query, version, name); err != nil {
			_ = tx.Rollback()
			return errors.Wrapf(err, "could not insert migration with version [%s] and name [%s] to [%s] table", version, name, g.migrationsTable)
		}
	}

	return tx.Commit()
}

func (g *MySQL) ShowTables(ctx context.Context) ([]string, error) {
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

func readVersions(tx *sql.Tx, migrationsTable string) ([]string, error) {
	rows, err := tx.Query(fmt.Sprintf("SELECT version FROM %s", migrationsTable))
	if err != nil {
		return nil, err
	}

	var result []string

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return result, err
		}
		result = append(result, version)
	}

	return result, nil
}

func inVersions(version string, versions []string) bool {
	for _, v := range versions {
		if v == version {
			return true
		}
	}

	return false
}
