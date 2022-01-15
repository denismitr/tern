package tern

import (
	"github.com/denismitr/tern/v3/migration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_createConfigurators(t *testing.T) {
	tt := []struct {
		name                  string
		expectedConfigurators int
		steps                 int
		versions              []string
	}{
		{
			name:                  "zero values",
			expectedConfigurators: 0,
		},
		{
			name:                  "both params",
			expectedConfigurators: 2,
			steps:                 3,
			versions:              []string{"1234567890", "1234567899"},
		},
		{
			name:                  "only versions",
			expectedConfigurators: 1,
			steps:                 0,
			versions:              []string{"1234567890", "1234567899"},
		},
		{
			name:                  "only steps",
			expectedConfigurators: 1,
			steps:                 4,
			versions:              []string{},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			configurators, err := CreateConfigurators(tc.steps, tc.versions)
			require.NoError(t, err)
			assert.Len(t, configurators, tc.expectedConfigurators)

			var a Action

			for _, c := range configurators {
				c(&a)
			}

			assert.Equal(t, tc.steps, a.steps)
			assert.Len(t, a.versions, len(tc.versions))

			for i := range tc.versions {
				assert.Equal(t, tc.versions[i], a.versions[i].Value)
			}
		})
	}
}

func Test_action(t *testing.T) {
	t.Parallel()

	t.Run("versions and steps", func(t *testing.T) {
		a := Action{}

		WithSteps(3)(&a)
		WithVersions(migration.Order{Value: "00000000000001"}, migration.Order{Value: "00000000000002"})(&a)

		assert.Equal(t, 3, a.steps)
		require.Len(t, a.versions, 2)
		assert.Equal(t, "00000000000001", a.versions[0].Value)
		assert.Equal(t, "00000000000002", a.versions[1].Value)
	})
}
