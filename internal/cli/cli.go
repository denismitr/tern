package cli

import (
	"context"
	"github.com/denismitr/tern"
	"github.com/denismitr/tern/migration"
	"github.com/denismitr/tern/source"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
	"time"
)

var (
	ErrMigrationAlreadyExists = errors.New("migration already exists")
	ErrFolderInvalid = errors.New("migrations folder is invalid")
	ErrSourceTypeIsNotValid = errors.New("source type is not valid")
)

type (
	CloserFunc func() error

	Config struct {
		DatabaseUrl      string
		MigrationsFolder string
	}

	ActionConfig struct {
		Steps   int
		Key     string
		Version string
	}

	App struct {
		source   source.Source
		migrator *tern.Migrator
	}
)

func NewFromYaml(path string) (*App, CloserFunc, error) {
	cfg, err := createConfigFromYaml(path)
	if err != nil {
		return nil, nil, err
	}

	return New(cfg)
}

func New(cfg Config) (*App, CloserFunc, error) {
	m, closer, err := createMigrator(cfg)
	if err != nil {
		return nil, nil, err
	}

	s := m.Source()
	if s == nil {
		return nil, nil, ErrSourceTypeIsNotValid
	}

	return &App{
		source: s,
		migrator: m,
	}, CloserFunc(closer), nil
}

func (app *App) CreateMigration(
	name string,
	vf migration.VersionFormat,
	withRollback bool,
) (*migration.Migration, error) {
	if !app.source.IsValid() {
		return nil, ErrFolderInvalid
	}

	v := migration.GenerateVersion(time.Now, vf)

	if app.source.AlreadyExists(v.Timestamp, name) {
		return nil, errors.Wrapf(ErrMigrationAlreadyExists, "dt [%s] name [%s]", v.Timestamp, name)
	}

	return app.source.Create(v.Timestamp, name, withRollback)
}

func (app *App) Migrate(ctx context.Context, cfg ActionConfig) error {
	var configurators []tern.ActionConfigurator
	if cfg.Steps > 0 {
		configurators = append(configurators, tern.WithSteps(cfg.Steps))
	}

	if _, err := app.migrator.Migrate(ctx, configurators...); err != nil {
		return err
	}

	return nil
}

func (app *App) Rollback(ctx context.Context, cfg ActionConfig) error {
	var configurators []tern.ActionConfigurator
	if cfg.Steps > 0 {
		configurators = append(configurators, tern.WithSteps(cfg.Steps))
	}

	if _, err := app.migrator.Rollback(ctx, configurators...); err != nil {
		return err
	}

	return nil
}

func (app *App) Refresh(ctx context.Context, cfg ActionConfig) error {
	var configurators []tern.ActionConfigurator
	if cfg.Steps > 0 {
		configurators = append(configurators, tern.WithSteps(cfg.Steps))
	}

	if _, _, err := app.migrator.Refresh(ctx, configurators...); err != nil {
		return err
	}

	return nil
}

func InitCfg(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrap(err, "could not create config file")
	}

	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()

	r := strings.NewReader(configFileStub)

	if _, err := io.Copy(f, r); err != nil {
		return err
	}

	return nil
}

func FileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
