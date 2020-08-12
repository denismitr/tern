package tern

type OptionFunc func(m *Migrator)

func UseLocalFolder(folder string) OptionFunc {
	return func(m *Migrator) {
		conv := localFSConverter{folder: folder}
		m.converter = conv
	}
}
