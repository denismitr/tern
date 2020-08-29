# TERN
## Another GO migrator

### WIP

### Embedded Usage
#### MySQL and sqlx

```go
db, err := sqlx.Open("mysql", testConnection)
if err != nil {
    panic(err)
}

defer db.Close()

m, err := tern.NewMigrator(
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
    panic(err)
}

ctx, cancel := context.WithTimeout(context.Background(), 20 * time.Second)
defer cancel()

migrated, err := m.Migrate(ctx); 
if err != nil {
    panic(err)
}

fmt.Printf("%#v", migrated)
// []string{"1596897167_create_foo_table", "1596897188_create_bar_table", "1597897177_create_baz_table"}
```

Apart from `Migrate` command, there are `Rollback` and `Refresh` commands.