package retry

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRetry(t *testing.T) {
	t.Run("single successful try", func(t *testing.T) {
		runs := 0

		err := Incremental(context.Background(), 2 * time.Millisecond, 5, func(attempt int) error {
			runs++
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, runs)
	})

	t.Run("success from the third time", func(t *testing.T) {
		runs := 0

		err := Incremental(context.Background(), 2 * time.Millisecond, 4, func(attempt int) error {
			runs++
			if attempt < 3 {
				return Error(errors.New("attempt failed"), attempt)
			}

			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 3, runs)
	})

	t.Run("fails when attempt limit is exhausted", func(t *testing.T) {
		runs := 0

		err := Incremental(context.Background(), 2 * time.Millisecond, 4, func(attempt int) error {
			runs++
			if attempt < 5 {
				return Error(errors.New("attempt failed"), attempt)
			}

			return nil
		})

		assert.Error(t, err)
		assert.Equal(t, 4, runs)
	})

	t.Run("fails if not an instance of retry error is returned from callback", func(t *testing.T) {
		runs := 0

		err := Incremental(context.Background(), 2 * time.Millisecond, 4, func(attempt int) error {
			runs++
			return errors.New("some error")
		})

		assert.Error(t, err)
		assert.Equal(t, 1, runs)
	})
}