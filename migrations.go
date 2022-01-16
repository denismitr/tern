package tern

type (
	Batch uint
	Order uint

	Version struct {
		Name       string
		Batch      Batch
		Order      Order
	}

	Migration struct {
		Version  Version
		Migrate  []string
		Rollback []string
	}

	Migrations []*Migration
)
