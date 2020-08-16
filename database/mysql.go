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

func (g *MySQL) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}

	return nil
}

func (g *MySQL) Up(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	if err := g.locker.lock(ctx, g.conn); err != nil {
		return nil, errors.Wrap(err, "migrations up lock failed")
	}

	defer func() {
		if err := g.locker.unlock(ctx, g.conn); err != nil {
			panic(err) // fixme
		}
	}()

	if err := g.CreateMigrationsTable(ctx); err != nil {
		return nil, err
	}

	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	versions, err := readVersions(tx, g.migrationsTable)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

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

	var migrated migration.Migrations
	for i := range scheduled {
		if err := up(ctx, tx, scheduled[i], insertVersionQuery); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return nil, errors.Wrap(err, rollbackErr.Error())
			}

			return nil, err
		}

		migrated = append(migrated, scheduled[i])
	}

	return migrated, tx.Commit()
}

func (g *MySQL) Down(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	if err := g.locker.lock(ctx, g.conn); err != nil {
		return nil, errors.Wrap(err, "down migrations lock failed")
	}

	defer func() {
		if err := g.locker.unlock(ctx, g.conn); err != nil {
			panic(err) // fixme
		}
	}()

	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	versions, err := readVersions(tx, g.migrationsTable)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	deleteVersionQuery := g.createDeleteVersionQuery()

	var executed migration.Migrations
	for i := range migrations {
		if inVersions(migrations[i].Version, versions) {
			if err := down(ctx, tx, migrations[i], deleteVersionQuery); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return nil, errors.Wrap(err, rollbackErr.Error())
				}

				return nil, err
			}

			executed = append(executed, migrations[i])
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "could not commit the down migration execution")
	}

	return executed, nil
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
