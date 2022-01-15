package tern

import "github.com/denismitr/tern/v3/migration"

type OptionFunc func(*Migrator) error
type ActionConfigurator func(a *Action)

type Action struct {
	steps    int
	versions []migration.Version
}

func WithSteps(steps int) ActionConfigurator {
	return func(a *Action) {
		a.steps = steps
	}
}

func WithVersions(versions ...migration.Version) ActionConfigurator {
	return func(a *Action) {
		a.versions = versions
	}
}

func CreateConfigurators(steps int, versionStrings []string) ([]ActionConfigurator, error) {
	var configurators []ActionConfigurator
	if steps > 0 {
		configurators = append(configurators, WithSteps(steps))
	}

	if len(versionStrings) > 0 {
		var versions []migration.Version
		for _, s := range versionStrings {
			if v, err := migration.VersionFromString(s); err != nil {
				return nil, err
			} else {
				versions = append(versions, v)
			}
		}
		configurators = append(configurators, WithVersions(versions...))
	}

	return configurators, nil
}
