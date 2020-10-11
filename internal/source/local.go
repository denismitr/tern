package source

import (
	"context"
	"github.com/denismitr/tern/migration"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

const DefaultMigrationsFolder = "./migrations"

const (
	defaultSqlExtension = "sql"

	migrateFileSuffix                = "migrate"
	rollbackFileSuffix               = "rollback"
	defaultMigrateFileFullExtension  = ".migrate.sql"
	defaultRollbackFileFullExtension = ".rollback.sql"

	timestampBasedVersionFormat = `^(?P<version>\d{9,11})(_\w+)?$`
	timestampBasedNameFormat = `^\d{9,12}_(?P<name>\w+[\w_-]+)?$`
	datetimeBasedVersionFormat = `^(?P<version>\d{14})(_\w+)?$`
	datetimeBasedNameFormat = `^\d{14}_(?P<name>\w+[\w_-]+)?$`
)

type ParsingRules func() (*regexp.Regexp, *regexp.Regexp, error)

type LocalFSConverter struct {
	folder string
	vf migration.VersionFormat
	versionRegexp *regexp.Regexp
	nameRegexp *regexp.Regexp
}

func (c *LocalFSConverter) Create(dt, name string, withRollback bool) (*migration.Migration, error) {
	key := migration.CreateKeyFromTimestampAndName(dt, name)
	migrateFilename := filepath.Join(c.folder, key +defaultMigrateFileFullExtension)
	mf, err := os.Create(migrateFilename)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create file [%s]", migrateFilename)
	}

	if err := mf.Close(); err != nil {
		return nil, err
	}

	m := &migration.Migration{
		Key: key,
		Name: name,
		Version: migration.Version{
			Value: dt,
		},
	}

	if withRollback {
		rollbackFilename := filepath.Join(c.folder, key +defaultRollbackFileFullExtension)
		rf, err := os.Create(rollbackFilename)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create file [%s]", rollbackFilename)
		}

		if err := rf.Close(); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func NewLocalFSSource(folder string, vf migration.VersionFormat) (*LocalFSConverter, error) {
	versionRegexp, nameRegexp, err := LocalFSParsingRules(vf)
	if err != nil {
		return nil, err
	}

	return &LocalFSConverter{
		folder: folder,
		versionRegexp: versionRegexp,
		nameRegexp: nameRegexp,

	}, nil
}

func (c *LocalFSConverter) IsValid() bool {
	info, err := os.Stat(c.folder)
	if os.IsNotExist(err) {
		return false
	}

	return info.IsDir()
}

func (c *LocalFSConverter) AlreadyExists(dt, name string) bool {
	key := migration.CreateKeyFromTimestampAndName(dt, name)
	filename := filepath.Join(c.folder, key +defaultMigrateFileFullExtension)
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func LocalFSParsingRules(vf migration.VersionFormat) (*regexp.Regexp, *regexp.Regexp, error) {
	var versionRegexFormat string
	var nameRegexFormat string

	if vf == migration.TimestampFormat {
		versionRegexFormat = timestampBasedVersionFormat
		nameRegexFormat = timestampBasedNameFormat
	} else if vf == migration.DatetimeFormat {
		versionRegexFormat = datetimeBasedVersionFormat
		nameRegexFormat = datetimeBasedNameFormat
	} else {
		return nil, nil, errors.New("invalid or undefined version format")
	}

	versionRegexp, err := regexp.Compile(versionRegexFormat)
	if err != nil {
		return nil, nil, err
	}

	nameRegexp, err := regexp.Compile(nameRegexFormat)
	if err != nil {
		return nil, nil, err
	}

	return versionRegexp, nameRegexp, nil
}

func (c *LocalFSConverter) Select(ctx context.Context, f Filter) (migration.Migrations, error) {
	keys, err := c.getAllKeysFromFolder(f.Keys)
	if err != nil {
		return nil, err
	}

	migrationsCh := make(chan *migration.Migration)
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

func (c *LocalFSConverter) readOne(key string) (*migration.Migration, error) {
	up := filepath.Join(c.folder, key+defaultMigrateFileFullExtension)
	down := filepath.Join(c.folder, key+defaultRollbackFileFullExtension)

	fUp, err := os.Open(up)
	if err != nil {
		return nil, err
	}

	defer fUp.Close()

	fDown, err := os.Open(down)
	if err != nil {
		return nil, err
	}

	defer fDown.Close()

	migrateContents, err := ioutil.ReadAll(fUp);
	if err != nil {
		return nil, err
	}

	rollbackContents, err := ioutil.ReadAll(fDown);
	if err != nil {
		return nil, err
	}

	return c.createMigration(key, migrateContents, rollbackContents)
}

func (c *LocalFSConverter) createMigration(key string, migrateContents, rollbackContents []byte) (*migration.Migration, error) {
	name := c.extractNameFromKey(key)
	version, err := c.extractVersionFromKey(key)
	if err != nil {
		return nil, err
	}

	return migration.NewMigrationFromFile(key, name, version, string(migrateContents), string(rollbackContents))
}

func (c *LocalFSConverter) extractVersionFromKey(key string) (migration.Version, error) {
	var result migration.Version
	matches := c.versionRegexp.FindStringSubmatch(key)
	if len(matches) < 2 {
		return result, ErrInvalidTimestamp
	}

	result.Value = matches[1]

	return result, nil
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

	if segments[2] != defaultSqlExtension || !(segments[1] == migrateFileSuffix || segments[1] == rollbackFileSuffix) {
		return "", ErrNotAMigrationFile
	}

	return segments[0], nil
}