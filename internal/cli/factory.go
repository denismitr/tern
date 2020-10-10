package cli

import (
	"github.com/denismitr/tern"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type (
	migratorFactory    func(cfg Config) (*tern.Migrator, tern.CloserFunc, error)
	migratorFactoryMap map[string]migratorFactory

	paths struct {
		LocalFolder string `yaml:"local_folder"`
		DatabaseURL string `yaml:"database_url"`
	}

	configFile struct {
		Version string `yaml:"version"`
		Paths   paths  `yaml:"paths"`
	}
)

func createConfigFromYaml(path string) (Config, error) {
	var cfg Config
	f, err := os.Open(path)
	if err != nil {
		return cfg, errors.Wrap(err, "could not open tern configuration file")
	}

	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return cfg, errors.Wrap(err, "could not read tern configuration file")
	}

	var cfgFile configFile
	if err := yaml.Unmarshal(b, &cfgFile); err != nil {
		return cfg, errors.Wrap(err, "could not parse tern configuration file")
	}

	if strings.HasSuffix(cfgFile.Paths.DatabaseURL, "%%") && strings.HasPrefix(cfgFile.Paths.DatabaseURL, "%%") {
		cfg.DatabaseUrl = os.Getenv(strings.ReplaceAll(cfgFile.Paths.DatabaseURL, "%%", ""))
	} else {
		cfg.DatabaseUrl = cfgFile.Paths.DatabaseURL
	}

	if strings.HasSuffix(cfgFile.Paths.LocalFolder, "%%") && strings.HasPrefix(cfgFile.Paths.LocalFolder, "%%") {
		cfg.MigrationsFolder = os.Getenv(strings.ReplaceAll(cfgFile.Paths.LocalFolder, "%%", ""))
	} else {
		cfg.MigrationsFolder = cfgFile.Paths.LocalFolder
	}

	if cfg.DatabaseUrl == "" {
		return cfg, errors.New("database url was not defined")
	}

	if cfg.MigrationsFolder == "" {
		return cfg, errors.New("migrations folder was not defined")
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
