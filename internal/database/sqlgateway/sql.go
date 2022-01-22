package sqlgateway

import (
	"context"
	"database/sql"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway/mysql"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway/postgres"
	"github.com/denismitr/tern/v3/internal/database/sqlgateway/sqlite"
	"github.com/denismitr/tern/v3/internal/logger"
	"github.com/pkg/errors"
	"time"
)

type SQLGateway struct {
	locker  Locker
	lg      logger.Logger
	conn    *sql.Conn
	dialect Dialect
}

var _ database.Effector = (*SQLGateway)(nil)

// NewMySQLGateway - creates a new MySQ
// Connect to the MySQL database
func NewMySQLGateway(
	conn *sql.Conn,
	migrationsTable string,
	lockKey string,
	lockFor int,
	noLock bool,
	charset string,
) *SQLGateway {
	gateway := SQLGateway{}
	gateway.conn = conn
	gateway.locker = mysql.NewLocker(lockKey, lockFor, noLock)

	if migrationsTable == "" {
		migrationsTable = database.DefaultMigrationsTable
	}

	gateway.dialect = mysql.NewDialect(migrationsTable, charset)

	return &gateway
}

func NewPostgresGateway(
	conn *sql.Conn,
	migrationsTable string,
	lockKey int,
	lockFor int,
	noLock bool,
	charset string,
) *SQLGateway {
	gateway := SQLGateway{}
	gateway.conn = conn
	gateway.locker = postgres.NewLocker(lockKey, lockFor, noLock)

	if migrationsTable == "" {
		migrationsTable = database.DefaultMigrationsTable
	}

	gateway.dialect = postgres.NewDialect(migrationsTable, charset)

	return &gateway
}

// NewSqliteGateway - creates a new SQL gateway
func NewSqliteGateway(conn *sql.Conn, migrationsTable string) *SQLGateway {
	gateway := SQLGateway{}
	gateway.conn = conn
	gateway.locker = &nullLocker{}

	if migrationsTable == "" {
		migrationsTable = database.DefaultMigrationsTable
	}

	gateway.dialect = sqlite.NewDialect(migrationsTable)

	return &gateway
}

func (g *SQLGateway) SetLogger(lg logger.Logger) {
	g.lg = lg
}

func (g *SQLGateway) Migrate(
	ctx context.Context,
	migrations database.Migrations,
	p database.Plan) (database.Migrations, error) {
	var migrated database.Migrations

	f := func(tx *sql.Tx, migratedVersions []database.Version) error {
		scheduled := database.ScheduleForMigration(migrations, migratedVersions, p)

		if len(scheduled) == 0 {
			return database.ErrNoChangesRequired
		}

		for i := range scheduled {
			scheduled[i].Version.MigratedAt = time.Now()
			if err := g.migrateOne(ctx, tx, scheduled[i]); err != nil {
				return err
			}

			g.lg.Successf(
				"migrated: version: %d batch: %d name: %s",
				scheduled[i].Version.ID, scheduled[i].Version.Batch, scheduled[i].Version.Name,
			)

			migrated = append(migrated, scheduled[i])
		}

		return nil
	}

	if err := g.execUnderLock(ctx, database.OperationMigrate, f); err != nil {
		return migrated, err
	}

	return migrated, nil
}

func (g *SQLGateway) Rollback(
	ctx context.Context,
	migrations database.Migrations,
	p database.Plan,
) (database.Migrations, error) {
	var rolledBack database.Migrations

	f := func(tx *sql.Tx, migratedVersions []database.Version) error {
		scheduled := database.ScheduleForRollback(migrations, migratedVersions, p)

		if len(scheduled) == 0 {
			return database.ErrNoChangesRequired
		}

		for i := range scheduled {
			g.lg.Debugf(
				"rolling back version: %d, batch %d, name %s",
				scheduled[i].Version.ID, scheduled[i].Version.Batch, scheduled[i].Version.Name,
			)

			if err := g.rollbackOne(ctx, tx, scheduled[i]); err != nil {
				return err
			}

			g.lg.Successf(
				"rolled back version: %d, batch %d, name %s",
				scheduled[i].Version.ID, scheduled[i].Version.Batch, scheduled[i].Version.Name,
			)

			rolledBack = append(rolledBack, scheduled[i])
		}

		return nil
	}

	if err := g.execUnderLock(ctx, database.OperationRollback, f); err != nil {
		return rolledBack, err
	}

	return rolledBack, nil
}

func (g *SQLGateway) Refresh(
	ctx context.Context,
	migrations database.Migrations,
	p database.Plan,
) (database.Migrations, database.Migrations, error) {
	var rolledBack database.Migrations
	var migrated database.Migrations

	f := func(tx *sql.Tx, migratedVersions []database.Version) error {
		scheduled := database.ScheduleForRefresh(migrations, migratedVersions, p)

		if len(scheduled) == 0 {
			return database.ErrNoChangesRequired
		}

		for i := range scheduled {
			g.lg.Debugf(
				"rolling back version: %d, batch %d, name %s",
				scheduled[i].Version.ID, scheduled[i].Version.Batch, scheduled[i].Version.Name,
			)

			if err := g.rollbackOne(ctx, tx, scheduled[i]); err != nil {
				return err
			}

			rolledBack = append(rolledBack, scheduled[i])
			g.lg.Successf(
				"rolled back version: %d, batch %d, name %s",
				scheduled[i].Version.ID, scheduled[i].Version.Batch, scheduled[i].Version.Name,
			)
		}

		for i := len(scheduled) - 1; i >= 0; i-- {
			g.lg.Debugf(
				"migrating version: %d, batch %d, name %s",
				scheduled[i].Version, scheduled[i].Version.Batch, scheduled[i].Version.Name,
			)

			scheduled[i].Version.MigratedAt = time.Now()
			if err := g.migrateOne(ctx, tx, scheduled[i]); err != nil {
				return err
			}

			migrated = append(migrated, scheduled[i])
			g.lg.Debugf("migrated version: %d, batch %d, name %s",
				scheduled[i].Version.ID, scheduled[i].Version.Batch, scheduled[i].Version.Name,
			)
		}

		return nil
	}

	if err := g.execUnderLock(ctx, database.OperationRefresh, f); err != nil {
		return rolledBack, migrated, err
	}

	return rolledBack, migrated, nil
}

func (g *SQLGateway) ReadVersions(ctx context.Context) ([]database.Version, error) {
	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	result, err := g.readVersionsUnderTx(tx, database.ReadVersionsFilter{Sort: ASC})
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
	if _, err := g.conn.ExecContext(ctx, g.dialect.InitQuery()); err != nil {
		return err
	}

	return nil
}

func (g *SQLGateway) DropMigrationsTable(ctx context.Context) error {
	if _, err := g.conn.ExecContext(ctx, g.dialect.DropQuery()); err != nil {
		return err
	}

	return nil
}

func (g *SQLGateway) WriteVersions(ctx context.Context, migrations database.Migrations) error {
	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrap(err, "could not start transaction to write migratrions")
	}

	for _, m := range migrations {
		insertQuery, args, err := g.dialect.InsertQuery(m)
		if err != nil {
			return err
		}

		if _, err := g.conn.ExecContext(ctx, insertQuery, args...); err != nil {
			execErr := errors.Wrapf(
				err,
				"could not insert migration with query %s and args %+v",
				insertQuery, args,
			)

			if errRb := tx.Rollback(); errRb != nil {
				return errors.Wrapf(
					errRb,
					"could not rollback write migration versions transaction after error %v",
					execErr,
				)
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
	rows, err := g.conn.QueryContext(ctx, g.dialect.ShowTablesQuery())
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

func (g *SQLGateway) execUnderLock(
	ctx context.Context,
	operation string,
	f func(*sql.Tx, []database.Version) error,
) error {
	if err := g.locker.Lock(ctx, g.conn); err != nil {
		return errors.Wrap(err, "database lock failed")
	}

	if err := g.CreateMigrationsTable(ctx); err != nil {
		return g.handleError(ctx, err, nil)
	}

	tx, err := g.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		err = errors.Wrapf(err, "could not start transaction to execute [%s] operation", operation)
		return g.handleError(ctx, err, nil)
	}

	availableVersions, err := g.readVersionsUnderTx(tx, database.ReadVersionsFilter{Sort: ASC})
	if err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			return g.handleError(ctx, err, tx)
		}

		return g.handleError(ctx, errors.Wrapf(err, "operation [%s] failed", operation), tx)
	}

	if err := f(tx, availableVersions); err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			return g.handleError(ctx, err, tx)
		}

		return g.handleError(ctx, errors.Wrapf(err, "operation [%s] failed", operation), tx)
	}

	if err := tx.Commit(); err != nil {
		err = errors.Wrapf(err, "could not commit [%s] operation, rolled back", operation)
		return g.handleError(ctx, err, tx)
	}

	return g.locker.Unlock(ctx, g.conn)
}

func (g *SQLGateway) migrateOne(ctx context.Context, ex CtxExecutor, m database.Migration) error {
	if m.Version.ID == 0 {
		return database.ErrMigrationVersionNotSpecified
	}

	insertQuery, args, err := g.dialect.InsertQuery(m)
	if err != nil {
		return err
	}

	if len(m.Migrate) > 0 {
		for _, script := range m.Migrate {
			g.lg.SQL(script)
			if _, err := ex.ExecContext(ctx, script); err != nil {
				return errors.Wrapf(
					err,
					"could not migrate script [%s], migration version: %d, batch %d, name %s",
					script, m.Version.ID, m.Version.Batch, m.Version.Name)
			}
		}
	}

	g.lg.SQL(insertQuery, args...)

	if _, err := ex.ExecContext(ctx, insertQuery, args...); err != nil {
		return errors.Wrapf(
			err,
			"could not insert migration version: %d, batch %d, name %s",
			m.Version.ID, m.Version.Batch, m.Version.Name,
		)
	}

	return nil
}

func (g *SQLGateway) rollbackOne(ctx context.Context, ex CtxExecutor, m database.Migration) error {
	if m.Version.ID == 0 {
		return database.ErrMigrationVersionNotSpecified
	}

	removeVersionQuery, args, err := g.dialect.RemoveQuery(m)
	if err != nil {
		return err
	}

	if len(m.Rollback) > 0 {
		for _, script := range m.Rollback {
			g.lg.SQL(script)
			if _, err := ex.ExecContext(ctx, script); err != nil {
				return errors.Wrapf(
					err,
					"could not rollback script [%s], migration version: %d, batch %d, name %s",
					script, m.Version.ID, m.Version.Batch, m.Version.Name,
				)
			}
		}
	}

	g.lg.SQL(removeVersionQuery, args...)

	if _, err := ex.ExecContext(ctx, removeVersionQuery, args...); err != nil {
		return errors.Wrapf(
			err,
			"could not remove migration version: %d, batch %d, name %s",
			m.Version.ID, m.Version.Batch, m.Version.Name,
		)
	}

	return nil
}

func (g *SQLGateway) readVersionsUnderTx(tx *sql.Tx, f database.ReadVersionsFilter) ([]database.Version, error) {
	q, err := g.dialect.ReadVersionsQuery(f)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(q)
	if err != nil {
		return nil, errors.Wrap(err, "could not execute query")
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, errors.Wrap(err, "read versions error")
	}

	var result []database.Version

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

		var order database.ID
		var batch database.Batch
		var name string
		var migratedAt time.Time
		if err := rows.Scan(&order, &batch, &name, &migratedAt); err != nil {
			if closeErr := rows.Close(); closeErr != nil {
				g.lg.Error(closeErr)
			}

			return result, err
		}

		result = append(result, database.Version{
			ID:         order,
			Batch:      batch,
			Name:       name,
			MigratedAt: migratedAt,
		})
	}

	return result, nil
}

func (g *SQLGateway) handleError(ctx context.Context, err error, tx *sql.Tx) error {
	var rollbackErr error
	var unlockErr error
	var result = err

	if tx != nil {
		rollbackErr = tx.Rollback()
		if rollbackErr != nil {
			result = errors.Wrapf(result, rollbackErr.Error())
		}
	}

	unlockErr = g.locker.Unlock(ctx, g.conn)
	if unlockErr != nil {
		result = errors.Wrapf(result, unlockErr.Error())
	}

	return result
}
