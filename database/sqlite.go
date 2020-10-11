package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/denismitr/tern/internal/logger"
	"github.com/denismitr/tern/migration"
	"time"
)

const (
	sqliteCreateMigrationsSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(13) PRIMARY KEY,
			name VARCHAR(255),
			migrated_at TIMESTAMP default CURRENT_TIMESTAMP
		);	
	`
	sqliteDeleteVersionQuery   = "DELETE FROM %s WHERE version = ?;"
	sqliteDropMigrationsSchema = "DROP TABLE IF EXISTS %s;"
	sqliteInsertVersionQuery   = "INSERT INTO %s (version, name) VALUES (?, ?);"
	sqliteShowTablesQuery      = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
)

type nullLocker struct {}

func (nullLocker) lock(ctx context.Context, conn *sql.Conn) error {
	return nil
}

func (nullLocker) unlock(ctx context.Context, conn *sql.Conn) error {
	return nil
}

type SqliteOptions struct {
	CommonOptions
}

type SqliteGateway struct {
	dbh             *dbh
}

var _ Gateway = (*SqliteGateway)(nil)
var _ ServiceGateway = (*SqliteGateway)(nil)

// NewSqliteGateway - creates a new Sqlite gateway and uses the connector interface to attempt to
// connect to the sqlite database
func NewSqliteGateway(connector connector, options *SqliteOptions) (*SqliteGateway, error) {
	dbh := newDbh(
		connector,
		&logger.NullLogger{},
		&nullLocker{},
		sqliteQueryBuilder{migrationsTable: options.MigrationsTable},
		options.MigrationsTable,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60 * time.Second) // fixme
	defer cancel()

	if err := dbh.connect(ctx); err != nil {
		return nil, err
	}

	return &SqliteGateway{
		dbh:   dbh,
	}, nil
}

func (g *SqliteGateway) WriteVersions(ctx context.Context, migrations migration.Migrations) error {
	return g.dbh.writeVersions(ctx, migrations)
}

func (g *SqliteGateway) ReadVersions(ctx context.Context) ([]migration.Version, error) {
	return g.dbh.readVersions(ctx)
}

func (g *SqliteGateway) ShowTables(ctx context.Context) ([]string, error) {
	return g.dbh.showTables(ctx)
}

func (g *SqliteGateway) DropMigrationsTable(ctx context.Context) error {
	return g.dbh.dropMigrationsTable(ctx)
}

func (g *SqliteGateway) CreateMigrationsTable(ctx context.Context) error {
	return g.dbh.createMigrationsTable(ctx)
}

func (g *SqliteGateway) Close() error {
	return g.dbh.close()
}

func (g *SqliteGateway) SetLogger(lg logger.Logger) {
	g.dbh.setLogger(lg)
}

func (g *SqliteGateway) Migrate(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	return g.dbh.migrateAll(ctx, migrations, p)
}

func (g *SqliteGateway) Rollback(ctx context.Context, migrations migration.Migrations, p Plan) (migration.Migrations, error) {
	return g.dbh.rollbackAll(ctx, migrations, p)
}

func (g *SqliteGateway) Refresh(ctx context.Context, migrations migration.Migrations, plan Plan) (migration.Migrations, migration.Migrations, error) {
	return g.dbh.refresh(ctx, migrations, plan)
}

type sqliteQueryBuilder struct {
	migrationsTable string
}

func (qb sqliteQueryBuilder) createMigrationsSchemaQuery() string {
	return fmt.Sprintf(sqliteCreateMigrationsSchema, qb.migrationsTable)
}

func (qb sqliteQueryBuilder) createShowTablesQuery() string {
	return sqliteShowTablesQuery
}

func (qb sqliteQueryBuilder) createDropMigrationsSchemaQuery() string {
	return fmt.Sprintf(sqliteDropMigrationsSchema, qb.migrationsTable)
}

func (qb sqliteQueryBuilder) createInsertVersionsQuery() string {
	return fmt.Sprintf(sqliteInsertVersionQuery, qb.migrationsTable)
}

func (qb sqliteQueryBuilder) createDeleteVersionQuery() string {
	return fmt.Sprintf(sqliteDeleteVersionQuery, qb.migrationsTable)
}
