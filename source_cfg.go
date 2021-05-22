package tern

import (
	"github.com/denismitr/tern/v2/internal/source"
	"github.com/denismitr/tern/v2/migration"
)

type (
	sourceConfig struct {
		versionFormat migration.VersionFormat
	}

	SourceConfigurator func(sc *sourceConfig)
)

func UseLocalFolderSource(folder string, configurators ...SourceConfigurator) OptionFunc {
	var sc sourceConfig
	sc.versionFormat = migration.AnyFormat
	for _, c := range configurators {
		c(&sc)
	}

	return func(m *Migrator) error {
		conv, err := source.NewLocalFSSource(folder, m.lg, sc.versionFormat)
		if err != nil {
			return err
		}

		m.selector = conv
		return nil
	}
}

func UseInMemorySource(factories ...migration.Factory) OptionFunc {
	return func(m *Migrator) error {
		s, err := source.NewInMemorySource(factories...)
		if err != nil {
			return err
		}

		m.selector = s
		return nil
	}
}

func WithVersionFormat(vf migration.VersionFormat) SourceConfigurator {
	return func(sc *sourceConfig) {
		sc.versionFormat = vf
	}
}
