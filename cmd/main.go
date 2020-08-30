package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/denismitr/tern"
	"github.com/jmoiron/sqlx"
	"github.com/logrusorgru/aurora/v3"
	"github.com/pkg/errors"
	"log"
	"os"
	"strings"
	"time"
)

type migratorFactory func (cfg config) (*tern.Migrator, error)
type commandFunc func (cfg config) error

type config struct {
	databaseUrl      string
	migrationsFolder string
}

type migratorFactoryMap map[string]migratorFactory

func migrate(cfg config) error {
	m, err := createMigrator(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40 * time.Second)
	defer cancel()

	_, err = m.Migrate(ctx)
	if err != nil {
		return err
	}

	return nil
}

func createMySQLMigrator(cfg config) (*tern.Migrator, error) {
	db, err := sqlx.Open("mysql", strings.TrimPrefix(cfg.databaseUrl, "mysql://"))
	if err != nil {
		return nil, err
	}

	var opts []tern.OptionFunc
	opts = append(
		opts,
		tern.UseMySQL(db.DB),
		tern.UseLocalFolderSource(cfg.migrationsFolder),
		tern.UseLogger(log.New(os.Stdout, "", log.Flags()), true, true),
	)

	return tern.NewMigrator(opts...)
}

func createMigrator(cfg config) (*tern.Migrator, error) {
	factoryMap := make(map[string]migratorFactory)
	factoryMap["mysql"] = createMySQLMigrator

	var driver string
	if strings.HasPrefix(cfg.databaseUrl, "mysql") {
		driver = "mysql"
	} else {
		panic(cfg.databaseUrl)
	}

	//parsedUrl, err := url.Parse(cfg.databaseUrl)
	//if err != nil {
	//	return nil, err
	//}

	//driver := parsedUrl.Scheme
	return createMigratorFrom(driver, factoryMap, cfg)
}

func createMigratorFrom(driver string, factoryMap migratorFactoryMap, cfg config) (*tern.Migrator, error) {
	factory, ok := factoryMap[driver]
	if ! ok {
		return nil, errors.Errorf("could not find factory for driver [%s]", driver)
	}

	return factory(cfg)
}

func main() {
	database := flag.String("db", "", "Database URL")
	folder := flag.String("folder", "", "local source folder, short for -source=file://path")
	migrateCmd := flag.Bool("migrate", true, "run the migrations")

	flag.Parse()

	if *database == "" {
		fmt.Println(aurora.Red("tern-cli: "), "Database not specified")
		os.Exit(1)
	}

	if *folder == "" {
		fmt.Println(aurora.Red("tern-cli: "), "Migrations folder not specified")
		os.Exit(1)
	}

	cfg := config{
		databaseUrl: *database,
		migrationsFolder: *folder,
	}

	if *migrateCmd {
		if err := migrate(cfg); err != nil {
			fmt.Println(aurora.Red("tern-cli: "), err.Error())
			os.Exit(1)
		}

		fmt.Println(aurora.Green("tern-cli: "), "all done")
		os.Exit(0)
	}

	fmt.Println(aurora.Red("tern-cli: "), "Unknown command")
	os.Exit(1)
}
