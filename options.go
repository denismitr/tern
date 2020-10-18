package tern

type OptionFunc func(*Migrator) error
type ActionConfigurator func(a *action)
