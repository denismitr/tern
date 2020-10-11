package tern

import (
	"github.com/denismitr/tern/internal/source"
	"github.com/denismitr/tern/migration"
)

type (
	sourceConfig struct {
		versionFormat migration.VersionFormat
	}

	SourceConfigurator func(sc *sourceConfig)
)

func UseLocalFolderSource(folder string, configurators ...SourceConfigurator) OptionFunc {
	var sc sourceConfig
	sc.versionFormat = migration.TimestampFormat
	for _, c := range configurators {
		c(&sc)
	}

	return func(m *Migrator) error {
		conv, err := source.NewLocalFSSource(folder, sc.versionFormat)
		if err != nil {
			return err
		}

		m.selector = conv
		return nil
	}
}

func UseInMemorySource(migrations ...*migration.Migration) OptionFunc {
	return func(m *Migrator) error {
		conv := source.NewInMemorySource(migrations...)

		m.selector = conv
		return nil
	}
}

func WithVersionFormat(vf migration.VersionFormat) SourceConfigurator {
	return func(sc *sourceConfig) {
		sc.versionFormat = vf
	}
}
