package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/denismitr/tern/database"
	"github.com/denismitr/tern/internal/cli"
	"github.com/denismitr/tern/migration"
	"github.com/logrusorgru/aurora/v3"
	"github.com/pkg/errors"
	"os"
	"time"
)

func main() {
	createCmd := flag.String("create", "", "create new migration")
	initCmd := flag.String("init-cfg", "./tern.yaml", "initialize Tern config file")
	migrateCmd := flag.Bool("migrate", false, "run the migrations")
	rollbackCmd := flag.Bool("rollback", false, "rollback the migrations")
	refreshCmd := flag.Bool("refresh", false, "refresh the migrations (rollback and then migrate)")
	configFile := flag.String("cfg", "./tern.yaml", "tern configuration file")

	versionFormat := flag.String("version-format", "timestamp", "Version format, that can be a timestamp or a datetime")
	noRollback := flag.Bool("no-rollback", false, "Create a new migration without a rollback")

	flag.Parse()

	if *initCmd != "" {
		createConfigFile(*initCmd)
	}

	if *configFile == "" {
		fmt.Println(aurora.Red("tern-cli: "), "Config file not specified")
		os.Exit(1)
	}

	app, closer, err := cli.NewFromYaml(*configFile)
	if err != nil {
		fmt.Println(aurora.Red("tern-cli: "), err.Error())
		os.Exit(1)
	}

	defer func() {
		if err := closer(); err != nil {
			panic(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second) // fixme
	defer cancel()

	if *createCmd != "" {
		var vf migration.VersionFormat
		if *versionFormat == "timestamp" {
			vf = migration.TimestampFormat
		} else {
			vf = migration.DatetimeFormat
		}

		m, err := app.CreateMigration(*createCmd, vf, !*noRollback);
		if err != nil {
			fmt.Println(aurora.Red("tern-cli: "), err.Error())
			os.Exit(1)
		}

		fmt.Println(aurora.Green("tern-cli: "), "Created migration " + m.Key)
		os.Exit(0)
	}

	if *migrateCmd {
		if err := app.Migrate(ctx, cli.ActionConfig{}); err != nil {
			if errors.Is(err, database.ErrNothingToMigrate) {
				fmt.Println(aurora.Green("tern-cli: "), "Nothing to migrate")
				os.Exit(0)
			}

			fmt.Println(aurora.Red("tern-cli: "), err.Error())
			os.Exit(1)
		}

		fmt.Println(aurora.Green("tern-cli: "), "all done")
		os.Exit(0)
	}

	if *rollbackCmd {
		if err := app.Refresh(ctx, cli.ActionConfig{}); err != nil {
			fmt.Println(aurora.Red("tern-cli: "), err.Error())
			os.Exit(1)
		}

		fmt.Println(aurora.Green("tern-cli: "), "all done")
		os.Exit(0)
	}

	if *refreshCmd {
		if err := app.Refresh(ctx, cli.ActionConfig{}); err != nil {
			fmt.Println(aurora.Red("tern-cli: "), err.Error())
			os.Exit(1)
		}

		fmt.Println(aurora.Green("tern-cli: "), "all done")
		os.Exit(0)
	}

	red("Unknown command")
	os.Exit(1)
}

func createConfigFile(filename string) {
	if cli.FileExists(filename) {
		return
	}

	green("creating config file: %s", filename)

	if err := cli.InitCfg(filename); err != nil {
		red(err.Error())
		os.Exit(1)
	}

	green("config file %s created. Done", filename)
	os.Exit(0)
}

func green(s string, f ...interface{}) {
	fmt.Printf(aurora.Green("tern-cli: ").String() + s, f...)
	fmt.Println()
}

func red(s string, f ...interface{}) {
	fmt.Printf(aurora.Red("tern-cli: ").String() + s, f...)
	fmt.Println()
}