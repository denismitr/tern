package tern

type OptionFunc func(m *Migrator)

func UseLocalFolder(folder string) OptionFunc {
	return func(m *Migrator) {
		conv := localFSConverter{folder: folder}
		m.converter = conv
	}
}

type action struct {
	steps int
}

type ActionConfigurator func (a *action)

func WithSteps(steps int) ActionConfigurator {
	return func (a *action) {
		a.steps = steps
	}
}
