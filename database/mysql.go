package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/denismitr/tern/logger"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
	"time"
)

const (
	mysqlCreateMigrationsSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(13) PRIMARY KEY,
			name VARCHAR(255),
			created_at TIMESTAMP default CURRENT_TIMESTAMP
		) ENGINE=INNODB;	
	`
	mysqlShowTables = "SHOW TABLES;"
	mysqlDeleteVersionQuery = "DELETE FROM %s WHERE version = ?;"
	mysqlDropMigrationsSchema = `DROP TABLE IF EXISTS %s;`
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

type mysqlQueryBuilder struct {
	migrationsTable string
}

func (qb mysqlQueryBuilder) createMigrationsSchemaQuery() string {
	return fmt.Sprintf(mysqlCreateMigrationsSchema, qb.migrationsTable)
}

func (qb mysqlQueryBuilder) createInsertVersionsQuery() string {
	return fmt.Sprintf(mysqlInsertVersionQuery, qb.migrationsTable)
}

func (qb mysqlQueryBuilder) createDeleteVersionQuery() string {
	return fmt.Sprintf(mysqlDeleteVersionQuery, qb.migrationsTable)
}

func (qb mysqlQueryBuilder) createShowTablesQuery() string {
	return mysqlShowTables
}

func (qb mysqlQueryBuilder) createDropMigrationsSchemaQuery() string {
	return fmt.Sprintf(mysqlDropMigrationsSchema, qb.migrationsTable)
}

type MySQLGateway struct {
	db              *dbh
}

var _ Gateway = (*MySQLGateway)(nil)
var _ ServiceGateway = (*MySQLGateway)(nil)

// NewMySQLGateway - creates a new MySQL gateway and uses the connector interface to attempt to
// connect to the MySQL database
func NewMySQLGateway(connector connector, options *MySQLOptions) (*MySQLGateway, error) {
	db := newDbh(
		connector,
		&logger.NullLogger{},
		&mySQLLocker{
			lockKey: options.LockKey,
			lockFor: options.LockFor,
		},
		mysqlQueryBuilder{migrationsTable: options.MigrationsTable},
		options.MigrationsTable,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60 * time.Second) // fixme
	defer cancel()

	if err := db.connect(ctx); err != nil {
		return nil, err
	}

	return &MySQLGateway{
		db: db,
	}, nil
}

// Close the connection
func (g *MySQLGateway) Close() error {
	return g.db.close()
}

func (g *MySQLGateway) SetLogger(lg logger.Logger) {
	g.db.setLogger(lg)
}

func (g *MySQLGateway) Migrate(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	return g.db.migrateAll(ctx, migrations, p)
}

func (g *MySQLGateway) Rollback(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	return g.db.rollbackAll(ctx, migrations, p)
}

func (g *MySQLGateway) Refresh(
	ctx context.Context,
	migrations migration.Migrations,
	plan Plan,
) (migration.Migrations, migration.Migrations, error) {
	return g.db.refresh(ctx, migrations, plan)
}

func (g *MySQLGateway) ReadVersions(ctx context.Context) ([]migration.Version, error) {
	return g.db.readVersions(ctx)
}

func (g *MySQLGateway) CreateMigrationsTable(ctx context.Context) error {
	return g.db.createMigrationsTable(ctx)
}

func (g *MySQLGateway) DropMigrationsTable(ctx context.Context) error {
	return g.db.dropMigrationsTable(ctx)
}

func (g *MySQLGateway) WriteVersions(ctx context.Context, migrations migration.Migrations) error {
	return g.db.writeVersions(ctx, migrations)
}

func (g *MySQLGateway) ShowTables(ctx context.Context) ([]string, error) {
	return g.db.showTables(ctx)
}
