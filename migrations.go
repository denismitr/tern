package tern

type (
	Batch uint
	ID    uint

	Version struct {
		Name  string
		Batch Batch
		ID    ID
	}

	Migration struct {
		Version  Version
		Migrate  []string
		Rollback []string
	}

	Migrations []*Migration
)
