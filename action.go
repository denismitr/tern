package tern

import (
	"github.com/denismitr/tern/v3/internal/database"
)

type OptionFunc func(*Migrator) error
type ActionConfigurator func(a *Action)

type Action struct {
	steps    int
	versions []database.Version
}

func WithSteps(steps int) ActionConfigurator {
	return func(a *Action) {
		a.steps = steps
	}
}

func WithVersions(versions ...Version) ActionConfigurator {
	return func(a *Action) {
		for i := range versions {
			a.versions = append(a.versions, database.Version{
				Name:  versions[i].Name,
				ID:    database.ID(versions[i].ID),
				Batch: database.Batch(versions[i].Batch),
			})
		}
	}
}

// TODO: refactor
func CreateConfigurators(steps int, versionOrders []uint) ([]ActionConfigurator, error) {
	var configurators []ActionConfigurator
	if steps > 0 {
		configurators = append(configurators, WithSteps(steps))
	}

	if len(versionOrders) > 0 {
		var versions []Version
		for _, order := range versionOrders {
			versions = append(versions, Version{
				ID: ID(order),
			})
		}
		configurators = append(configurators, WithVersions(versions...))
	}

	return configurators, nil
}
