package retry

import (
	"context"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRetry(t *testing.T) {
	t.Run("single successful try returns result", func(t *testing.T) {
		runs := 0

		result, err := Incremental(
			context.Background(),
			2*time.Millisecond,
			5,
			func(attempt int) (interface{}, error) {
				runs++
				return 25, nil
			})

		require.NoError(t, err)

		number, ok := result.(int)
		require.True(t, ok)
		assert.Equal(t, 25, number)
		assert.Equal(t, 1, runs)
	})

	t.Run("success from the third time", func(t *testing.T) {
		runs := 0

		result, err := Incremental(
			context.Background(),
			2*time.Millisecond,
			4,
			func(attempt int) (interface{}, error) {
				runs++
				if attempt < 3 {
					return nil, Error(errors.New("attempt failed"), attempt)
				}

				return "foo", nil
			})

		require.NoError(t, err)

		number, ok := result.(string)
		require.True(t, ok)
		assert.Equal(t, "foo", number)
		assert.Equal(t, 3, runs)
	})

	t.Run("fails when attempt limit is exhausted", func(t *testing.T) {
		runs := 0

		result, err := Incremental(
			context.Background(),
			2*time.Millisecond,
			4,
			func(attempt int) (interface{}, error) {
				runs++
				if attempt < 5 {
					return nil, Error(errors.New("attempt failed"), attempt)
				}

				return "bar", nil
			})

		require.Error(t, err)
		require.Nil(t, result)
		assert.Equal(t, 4, runs)
	})

	t.Run("fails if not an instance of retry error is returned from callback", func(t *testing.T) {
		runs := 0

		result, err := Incremental(
			context.Background(),
			2*time.Millisecond,
			4,
			func(attempt int) (interface{}, error) {
				runs++
				return nil, errors.New("some error")
			})

		require.Error(t, err)
		require.Nil(t, result)
		assert.Equal(t, 1, runs)
	})

	t.Run("it fails if context deadline exceeded", func(t *testing.T) {
		runs := 0

		ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
		defer cancel()

		result, err := Incremental(
			ctx,
			500*time.Millisecond,
			4,
			func(attempt int) (interface{}, error) {
				runs++
				return nil, Error(errors.New("attempt failed"), attempt)
			})

		require.Error(t, err)
		require.Nil(t, result)
		assert.True(t, errors.Is(err, ErrContextEnded))
	})
}
