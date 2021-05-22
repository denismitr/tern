package cli

import (
	"github.com/denismitr/tern/v2"
	"github.com/denismitr/tern/v2/migration"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type (
	migratorFactory    func(cfg Config) (*tern.Migrator, tern.CloserFunc, error)
	migratorFactoryMap map[string]migratorFactory

	migrations struct {
		LocalFolder   string `yaml:"local_folder"`
		DatabaseURL   string `yaml:"database_url"`
		VersionFormat string `yaml:"version_format"`
	}

	configFile struct {
		Version    string     `yaml:"version"`
		Migrations migrations `yaml:"migrations"`
	}
)

var (
	allowedVersionFormats = []migration.VersionFormat{migration.TimestampFormat, migration.DatetimeFormat}
)

func createConfigFromYaml(path string) (Config, error) {
	var cfg Config
	f, err := os.Open(path)
	if err != nil {
		return cfg, errors.Wrap(err, "could not open tern configuration file")
	}

	defer func() {
		if errClose := f.Close(); errClose != nil {
			panic(errClose)
		}
	}()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return cfg, errors.Wrap(err, "could not read tern configuration file")
	}

	var cfgFile configFile
	if err := yaml.Unmarshal(b, &cfgFile); err != nil {
		return cfg, errors.Wrap(err, "could not parse tern configuration file")
	}

	if strings.HasSuffix(cfgFile.Migrations.DatabaseURL, "%%") && strings.HasPrefix(cfgFile.Migrations.DatabaseURL, "%%") {
		cfg.DatabaseUrl = os.Getenv(strings.ReplaceAll(cfgFile.Migrations.DatabaseURL, "%%", ""))
	} else {
		cfg.DatabaseUrl = cfgFile.Migrations.DatabaseURL
	}

	if strings.HasSuffix(cfgFile.Migrations.LocalFolder, "%%") && strings.HasPrefix(cfgFile.Migrations.LocalFolder, "%%") {
		cfg.MigrationsFolder = os.Getenv(strings.ReplaceAll(cfgFile.Migrations.LocalFolder, "%%", ""))
	} else {
		cfg.MigrationsFolder = cfgFile.Migrations.LocalFolder
	}

	if cfg.DatabaseUrl == "" {
		return cfg, errors.New("database url was not defined")
	}

	if cfg.MigrationsFolder == "" {
		return cfg, errors.New("migrations folder was not defined")
	}

	for _, format := range allowedVersionFormats {
		if string(format) == cfgFile.Migrations.VersionFormat {
			cfg.VersionFormat = format
		}
	}

	if cfg.VersionFormat == "" {
		return cfg, ErrInvalidVersionFormat
	}

	return cfg, nil
}

func createMySQLMigrator(cfg Config) (*tern.Migrator, tern.CloserFunc, error) {
	db, err := sqlx.Open("mysql", strings.TrimPrefix(cfg.DatabaseUrl, "mysql://"))
	if err != nil {
		return nil, nil, err
	}

	var opts []tern.OptionFunc
	opts = append(
		opts,
		tern.UseMySQL(db.DB),
		tern.UseLocalFolderSource(cfg.MigrationsFolder),
		tern.UseColorLogger(log.New(os.Stdout, "", 0), true, true),
	)

	return tern.NewMigrator(opts...)
}

func createMigrator(cfg Config) (*tern.Migrator, tern.CloserFunc, error) {
	factoryMap := make(map[string]migratorFactory)
	factoryMap["mysql"] = createMySQLMigrator

	var driver string
	if strings.HasPrefix(cfg.DatabaseUrl, "mysql") {
		driver = "mysql"
	} else if strings.HasPrefix(cfg.DatabaseUrl, "sqlite") {
		driver = "sqlite"
	} else {
		return nil, nil, errors.Errorf("unknown database driver [%s]", cfg.DatabaseUrl)
	}

	return createMigratorFrom(driver, factoryMap, cfg)
}

func createMigratorFrom(
	driver string,
	factoryMap migratorFactoryMap,
	cfg Config,
) (*tern.Migrator, tern.CloserFunc, error) {
	factory, ok := factoryMap[driver]
	if !ok {
		return nil, nil, errors.Errorf("could not find factory for driver [%s]", driver)
	}

	return factory(cfg)
}
