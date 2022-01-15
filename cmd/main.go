package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/denismitr/tern/v3"
	"github.com/denismitr/tern/v3/internal/cli"
	"github.com/denismitr/tern/v3/internal/database"
	"github.com/logrusorgru/aurora/v3"
	"github.com/pkg/errors"
	"os"
	"strings"
	"time"
)

const defaultTimeout = 360

func main() {
	initFlag := flag.Bool("init", false, "initialize Tern config file")
	configFile := flag.String("cfg", "./tern.yaml", "tern configuration file")

	createCmd := flag.String("create", "", "create new migration")
	noRollback := flag.Bool("no-rollback", false, "Create a new migration without a rollback")

	migrateFlag := flag.Bool("migrate", false, "run the migrations")
	rollbackFlag := flag.Bool("rollback", false, "rollback the migrations")
	refreshFlag := flag.Bool("refresh", false, "refresh the migrations (rollback and then migrate again)")

	timeout := flag.Int("timeout", defaultTimeout, "max timeout")
	steps := flag.Int("steps", 0, "steps to execute")
	versionList := flag.String("versions", "", "version list (comma separated) to perform action on")

	flag.Parse()

	if *configFile == "" {
		exitWithError(errors.New("Config file not specified"))
	}

	if *initFlag {
		createConfigFile(*configFile)
		return
	}

	var versions []string
	if *versionList != "" {
		versions = strings.Split(*versionList, ",")
	}

	if *steps != 0 && len(versions) > 0 {
		exitWithError(errors.New("choose between using steps and versions, you cannot have both"))
	}

	app, closer, err := cli.NewFromYaml(*configFile)
	if err != nil {
		exitWithError(err)
	}

	defer func() {
		if err := closer(); err != nil {
			exitWithError(err)
		}
	}()

	if *createCmd != "" {
		createMigration(app, createCmd, noRollback)
		return
	}

	if *migrateFlag {
		migrate(app, *steps, versions, *timeout)
		return
	}

	if *rollbackFlag {
		rollback(app, *steps, versions, *timeout)
		return
	}

	if *refreshFlag {
		refresh(app, *steps, versions, *timeout)
		return
	}

	exitWithError(errors.New("You need to choose on of commands: init-cfg, create, migrate, rollback, refresh"))
}

func refresh(app *cli.App, steps int, versions []string, timeout int) {
	if timeout <= 0 {
		exitWithError(errors.New("refresh timeout must be a positive integer or simply be omitted"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if err := app.Refresh(ctx, steps, versions); err != nil {
		exitWithError(err)
	}

	green("Migration refresh completed. All done...")
}

func rollback(app *cli.App, steps int, versions []string, timeout int) {
	if timeout <= 0 {
		exitWithError(errors.New("rollback timeout must be a positive integer or simply be omitted"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if err := app.Rollback(ctx, steps, versions); err != nil {
		exitWithError(err)
	}

	green("Migration rollback completed. All done...")
}

func migrate(app *cli.App, steps int, versions []string, timeout int) {
	if timeout <= 0 {
		exitWithError(errors.New("migrate timeout must be a positive integer or simply be omitted"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if err := app.Migrate(ctx, steps, versions); err != nil {
		if errors.Is(err, database.ErrNoChangesRequired) {
			green("Nothing to migrate")
			return
		}

		exitWithError(err)
	}

	green("Migration complete. All done...")
}

func createMigration(app *cli.App, createCmd *string, noRollback *bool) {
	m, err := app.CreateMigration(*createCmd, !*noRollback)
	if err != nil {
		exitWithError(err)
	}

	green("Migration %s created ", m.Key)
}

func createConfigFile(filename string) {
	if cli.FileExists(filename) {
		green("config file %s already exists", filename)
		return
	}

	green("creating config file: %s", filename)

	if err := cli.InitCfg(filename); err != nil {
		exitWithError(err)
	}

	green("config file %s created. Done", filename)
}

func green(s string, f ...interface{}) {
	fmt.Printf(aurora.Green("tern-cli: ").String()+s, f...)
	fmt.Println()
}

func red(s string, f ...interface{}) {
	fmt.Printf(aurora.Red("tern-cli: ").String()+s, f...)
	fmt.Println()
}

func exitWithError(err error) {
	if errors.Is(err, tern.ErrNothingToMigrateOrRollback) {
		green("Nothing to migrate or rollback")
		os.Exit(0)
	}

	red(err.Error())
	red("tern terminated with error")
	os.Exit(1)
}
