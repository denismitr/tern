package tern

import (
	"context"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"unicode"
)

const defaultUpExtension = ".up.sql"
const defaultDownExtension = ".down.sql"

var ErrNotAMigrationFile = errors.New("not a migration file")
var ErrTooManyFilesForKey = errors.New("too many files for single key")

type filter struct {
	keys  []string
}

type converter interface {
	Convert(ctx context.Context, f filter) (Migrations, error)
}

type localFSConverter struct {
	folder string
}

func (c localFSConverter) Convert(ctx context.Context, f filter) (Migrations, error) {
	keys, err := c.getAllKeysFromFolder(f.keys)
	if err != nil {
		return nil, err
	}

	migrationsCh := make(chan Migration)
	var wg sync.WaitGroup

	for k := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			m, err := c.readOne(key)
			if err != nil {
				log.Printf("Migration error: %s", err.Error())
			}

			migrationsCh <- m
		}(k)
	}

	go func() {
		wg.Wait()
		close(migrationsCh)
	}()

	var result Migrations

	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case m, ok := <-migrationsCh:
			if ok {
				result = append(result, m)
			} else {
				sort.Sort(result)
				return filterMigrations(result, f), nil
			}
		}
	}
}

func filterMigrations(m Migrations, f filter) Migrations {
	return m
}

func (c localFSConverter) getAllKeysFromFolder(onlyKeys []string) (map[string]int, error) {
	files, err := ioutil.ReadDir(c.folder)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read keys from folder %s", c.folder)
	}

	keys := make(map[string]int)

	for i := range files {
		if files[i].IsDir() {
			continue
		}

		key, err := convertLocalFilePathToKey(files[i].Name())
		if err != nil {
			return nil, errors.Wrapf(err, "file %s is not a valid migration name", files[i].Name()) // fixme
		}

		if len(onlyKeys) > 0 && !inStringSlice(key, onlyKeys) {
			continue
		}

		if count, ok := keys[key]; ok {
			keys[key] = count + 1
			if keys[key] > 2 {
				return nil, errors.Wrapf(ErrTooManyFilesForKey, "%s", key)
			}
		} else {
			keys[key] = 1
		}
	}

	return keys, nil
}

func inStringSlice(key string, keys []string) bool {
	for i := range keys {
		if keys[i] == key {
			return true
		}
	}
	return false
}

func (c localFSConverter) readOne(key string) (Migration, error) {
	var result Migration

	up := filepath.Join(c.folder, key+defaultUpExtension)
	down := filepath.Join(c.folder, key+defaultDownExtension)

	fUp, err := os.Open(up)
	if err != nil {
		return result, err
	}

	defer fUp.Close()

	fDown, err := os.Open(down)
	if err != nil {
		return result, err
	}

	defer fDown.Close()

	if upContents, err := ioutil.ReadAll(fUp); err != nil {
		return result, err
	} else {
		result.Up = string(upContents)
	}

	if downContents, err := ioutil.ReadAll(fDown); err != nil {
		return result, err
	} else {
		result.Down = string(downContents)
	}

	result.Key = key
	result.Name = extractNameFromKey(key, nameRegexp) // fixme: no error
	result.Version, err = extractVersionFromKey(key, versionRegexp)
	if err != nil {
		return result, err
	}

	return result, nil
}

func extractVersionFromKey(key string, r *regexp.Regexp) (string, error) {
	matches := r.FindStringSubmatch(key)
	if len(matches) < 2 {
		return "", ErrInvalidTimestamp
	}

	return matches[1], nil
}

func extractNameFromKey(key string, r *regexp.Regexp) string {
	matches := r.FindStringSubmatch(key)
	if len(matches) < 2 {
		return ""
	}

	return ucFirst(strings.Replace(matches[1], "_", " ", -1))
}

func convertLocalFilePathToKey(path string) (string, error) {
	_, name := filepath.Split(path)
	base := filepath.Base(name)
	segments := strings.Split(base, ".")

	if len(segments) != 3 {
		return "", ErrNotAMigrationFile
	}

	if segments[2] != "sql" || !(segments[1] == "up" || segments[1] == "down") {
		return "", ErrNotAMigrationFile
	}

	return segments[0], nil
}

func ucFirst(s string) string {
	r := []rune(s)

	if len(r) == 0 {
		return ""
	}

	f := string(unicode.ToUpper(r[0]))

	return f + string(r[1:])
}
