package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/denismitr/tern/database"
	"github.com/denismitr/tern/internal/cli"
	"github.com/logrusorgru/aurora/v3"
	"github.com/pkg/errors"
	"os"
	"time"
)

const defaultTimeout = 360

func main() {
	initCmd := flag.String("init-cfg", "./tern.yaml", "initialize Tern config file")
	configFile := flag.String("cfg", "./tern.yaml", "tern configuration file")

	createCmd := flag.String("create", "", "create new migration")
	noRollback := flag.Bool("no-rollback", false, "Create a new migration without a rollback")

	migrateCmd := flag.Bool("migrate", false, "run the migrations")
	rollbackCmd := flag.Bool("rollback", false, "rollback the migrations")
	refreshCmd := flag.Bool("refresh", false, "refresh the migrations (rollback and then migrate again)")

	timeout := flag.Int("timeout", defaultTimeout, "max timeout")
	steps := flag.Int("steps", 0, "steps to execute")

	flag.Parse()

	if *initCmd != "" {
		createConfigFile(*initCmd)
	}

	if *configFile == "" {
		red("Config file not specified")
		os.Exit(1)
	}

	app, closer, err := cli.NewFromYaml(*configFile)
	if err != nil {
		red(err.Error())
		os.Exit(1)
	}

	defer func() {
		if err := closer(); err != nil {
			panic(err)
		}
	}()

	if *createCmd != "" {
		createMigration(app, createCmd, noRollback)
	}

	if *migrateCmd {
		migrate(app, *steps, *timeout)
	}

	if *rollbackCmd {
		rollback(app, *steps, *timeout)
	}

	if *refreshCmd {
		refresh(app, *steps, *timeout)
	}

	red("You need to choose on of commands: init-cfg, create, migrate, rollback, refresh")
	os.Exit(1)
}

func refresh(app *cli.App, steps, timeout int) {
	if timeout <= 0 {
		red("refresh timeout must be a positive integer or simply be omitted")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if err := app.Rollback(ctx, cli.ActionConfig{Steps: steps}); err != nil {
		red(err.Error())
		os.Exit(1)
	}

	green("Migration refresh completed. All done...")
	os.Exit(0)
}

func rollback(app *cli.App, steps, timeout int) {
	if timeout <= 0 {
		red("rollback timeout must be a positive integer or simply be omitted")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if err := app.Rollback(ctx, cli.ActionConfig{Steps: steps}); err != nil {
		red(err.Error())
		os.Exit(1)
	}

	green("Migration rollback completed. All done...")
	os.Exit(0)
}

func migrate(app *cli.App, steps, timeout int) {
	if timeout <= 0 {
		red("migrate timeout must be a positive integer or simply be omitted")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if err := app.Migrate(ctx, cli.ActionConfig{Steps: steps}); err != nil {
		if errors.Is(err, database.ErrNothingToMigrate) {
			green("Nothing to migrate")
			os.Exit(0)
		}

		red(err.Error())
		os.Exit(1)
	}

	green("Migration complete. All done...")
	os.Exit(0)
}

func createMigration(app *cli.App, createCmd *string, noRollback *bool) {
	m, err := app.CreateMigration(*createCmd, !*noRollback)
	if err != nil {
		red(err.Error())
		os.Exit(1)
	}

	green("Migration %s created ", m.Key)
	os.Exit(0)
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