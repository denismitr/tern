# TERN
## Another GO migrator

## Version 1.*

### Supported databases - MySQL, Sqlite
Postgres is TODO, PRs are welcomed (with tests!!!)

## CLI Usage
#### Initialize config file
```bash
tern-cli -init
```
or
```bash
tern-cli -init -cfg tern.yaml
```

#### Tern default config file
```yaml
version: "1"

migrations:
  local_folder: "./migrations"
  database_url: "mysql://username:password@(127.0.0.1:3306)/your_db_name?parseTime=true"
  version_format: datetime
```

#### Create a new migration
```bash
tern-cli -create update_foo_table
```

will create 2 files in your migration folder
```bash
1602439886_update_foo_table.migrate.sql
1602439886_update_foo_table.rollback.sql
```

#### Migrate
```bash
tern-cli -migrate
```
you can specify a number of steps (files) to execute and/or a timeout
```bash
tern-cli -migrate -steps 2 -timeout 30
```
That will run the first 2 migrations, that had not been migrated before, and will set timeout of 30s 


#### Rollback
```bash
tern-cli -rollback
```
you can specify a number of steps (files) to execute and/or a timeout
```bash
tern-cli -rollback -steps 2 -timeout 30
```
That will rollback the latest 2 migrations, and will set timeout of 30s

#### Refresh
will rollback and then migrate again only those migrations that had been previously applied to the database
```bash
tern-cli -refresh
```
you can specify a number of steps (files) to rollback and then migrate again and/or a timeout
```bash
tern-cli -refresh -steps 2 -timeout 30
```
That will refresh (rollback and migrate again) the latest 2 migrations, and will set timeout of 30s

### Embedded Usage
#### MySQL and sqlx

```go
db, err := sqlx.Open("mysql", "username:secret@(127.0.0.1:33066)/my_db_name?parseTime=true")
if err != nil {
    panic(err)
}

defer db.Close()

m, closer, err := tern.NewMigrator(
    tern.UseMySQL(db.DB), // db.DB is actually *sql.DB
    tern.UseLocalFolderSource("./migrations"),
)

// ./migrations folder contents
// 1596897167_create_foo_table.migrate.sql
// 1596897167_create_foo_table.rollback.sql
// 1596897188_create_bar_table.migrate.sql
// 1596897188_create_bar_table.rollback.sql
// 1597897177_create_baz_table.migrate.sql
// 1597897177_create_baz_table.rollback.sql

if err != nil {
	if errors.Is(err, tern.ErrNothingToMigrateOrRollback) {
		os.Exit(0)
    }   
    panic(err)
}

defer closer()

ctx, cancel := context.WithTimeout(context.Background(), 20 * time.Second)
defer cancel()

migrated, err := m.Migrate(ctx); 
if err != nil {
    if errors.Is(err, tern.ErrNothingToMigrateOrRollback) {
        os.Exit(0)
    }
    panic(err)
}

fmt.Printf("%#v", migrated)
// []string{"1596897167_create_foo_table", "1596897188_create_bar_table", "1597897177_create_baz_table"}
```

Apart from `Migrate` command, there are `Rollback` and `Refresh` commands.

#### In memory source

```go
package main

import (
	"context"
	"fmt"
	"os"
	"github.com/denismitr/tern/v2"
	"github.com/denismitr/tern/v2/migration"
	"database/sql"
	"github.com/pkg/errors"
)

func main() {
	db, err := sql.Open("mysql", "tern:secret@(127.0.0.1:33066)/tern_db?parseTime=true")
	if err != nil {
		panic(err)
	}

	source := tern.UseInMemorySource(
		migration.New(
			migration.DateTime(2020, 10, 11, 22, 17, 45),
			"Create foo table",
			[]string{"CREATE TABLE IF NOT EXISTS foo (id binary(16) PRIMARY KEY) ENGINE=INNODB;"},
			[]string{"DROP TABLE IF EXISTS foo;"},
		),
		migration.New(
			migration.DateTime(2020, 10, 11, 22, 21, 45),
			"Add price column",
			[]string{"ALTER TABLE foo ADD price INT UNSIGNED NOT NULL DEFAULT 0"},
			[]string{"ALTER TABLE foo DROP price"},
		),
	)

	m, closer, err := tern.NewMigrator(
		tern.UseMySQL(
			db,
			tern.WithMySQLMigrationTable("myapp_migrations"),
			tern.WithMySQLNoLock(), // for master-master replication
		),
		source,
	)

	if err != nil {
		panic(err)
	}

	defer closer()

	migrated, err := m.Migrate(context.Background())
	if err != nil {
		if errors.Is(err, tern.ErrNothingToMigrateOrRollback) {
			os.Exit(0)
		}
		panic(err)
	}

	for i := range migrated {
		fmt.Printf("\nMigrated SQL %s", migrated[i])
	}
}

```