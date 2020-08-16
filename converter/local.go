package converter

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const DefaultMigrationsFolder = "./migrations"

type LocalFSConverter struct {
	folder string
	versionRegexp *regexp.Regexp
	nameRegexp *regexp.Regexp
}

func NewLocalFSConverter(folder string) (*LocalFSConverter, error) {
	versionRegexp, err := regexp.Compile(`^(?P<version>\d{1,12})(_\w+)?$`)
	if err != nil {
		return nil, err
	}
	nameRegexp, err := regexp.Compile(`^\d{1,12}_(?P<name>\w+[\w_-]+)?$`)
	if err != nil {
		return nil, err
	}

	return &LocalFSConverter{
		folder: folder,
		versionRegexp: versionRegexp,
		nameRegexp: nameRegexp,
	}, nil
}

func (c *LocalFSConverter) Convert(ctx context.Context, f Filter) (migration.Migrations, error) {
	keys, err := c.getAllKeysFromFolder(f.keys)
	if err != nil {
		return nil, err
	}

	migrationsCh := make(chan migration.Migration)
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

	var result migration.Migrations

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

func (c *LocalFSConverter) getAllKeysFromFolder(onlyKeys []string) (map[string]int, error) {
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

func (c *LocalFSConverter) readOne(key string) (migration.Migration, error) {
	var result migration.Migration

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
	result.Name = c.extractNameFromKey(key)
	result.Version, err = c.extractVersionFromKey(key)
	if err != nil {
		return result, err
	}

	return result, nil
}

func (c *LocalFSConverter) extractVersionFromKey(key string) (string, error) {
	matches := c.versionRegexp.FindStringSubmatch(key)
	if len(matches) < 2 {
		return "", ErrInvalidTimestamp
	}

	return matches[1], nil
}

func (c *LocalFSConverter) extractNameFromKey(key string) string {
	matches := c.nameRegexp.FindStringSubmatch(key)
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