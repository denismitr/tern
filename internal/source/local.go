package source

import (
	"context"
	"github.com/denismitr/tern/v2/internal/logger"
	"github.com/denismitr/tern/v2/migration"
	"github.com/pkg/errors"
	"io/ioutil"
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
	timestampBasedNameFormat = `^\d{9,11}_(?P<name>\w+[\w_-]+)?$`
	datetimeBasedVersionFormat = `^(?P<version>\d{14})(_\w+)?$`
	datetimeBasedNameFormat = `^\d{14}_(?P<name>\w+[\w_-]+)?$`

	anyBasedVersionFormat = `^(?P<version>\d{9,14})(_\w+)?$`
	anyBasedNameFormat = `^\d{9,14}_(?P<name>\w+[\w_-]+)?$`
)

type ParsingRules func() (*regexp.Regexp, *regexp.Regexp, error)

type LocalFileSource struct {
	folder        string
	lg            logger.Logger
	versionRegexp *regexp.Regexp
	nameRegexp    *regexp.Regexp
	versionFormat migration.VersionFormat
}

func (lfs *LocalFileSource) Create(dt, name string, withRollback bool) (*migration.Migration, error) {
	key := migration.CreateKeyFromVersionAndName(dt, name)
	migrateFilename := filepath.Join(lfs.folder, key + defaultMigrateFileFullExtension)
	mf, err := os.Create(migrateFilename)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create file [%s]", migrateFilename)
	}

	if cErr := mf.Close(); cErr != nil {
		return nil, errors.Wrapf(cErr, "could not close file %s", migrateFilename)
	}

	m := &migration.Migration{
		Key: key,
		Name: name,
		Version: migration.Version{
			Value: dt,
			Format: lfs.versionFormat,
		},
	}

	if withRollback {
		rollbackFilename := filepath.Join(lfs.folder, key + defaultRollbackFileFullExtension)
		rf, err := os.Create(rollbackFilename)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create file [%s]", rollbackFilename)
		}

		if cErr := rf.Close(); cErr != nil {
			return nil, errors.Wrapf(cErr, "could not close file %s", rollbackFilename)
		}
	}

	return m, nil
}

func NewLocalFSSource(
	folder string,
	lg logger.Logger,
	vf migration.VersionFormat,
) (*LocalFileSource, error) {
	versionRegexp, nameRegexp, err := LocalFSParsingRules(vf)
	if err != nil {
		return nil, err
	}

	return &LocalFileSource{
		folder: folder,
		versionRegexp: versionRegexp,
		nameRegexp: nameRegexp,
		versionFormat: vf,
		lg: lg,
	}, nil
}

func (lfs *LocalFileSource) IsValid() bool {
	info, err := os.Stat(lfs.folder)
	if os.IsNotExist(err) {
		return false
	}

	return info.IsDir()
}

func (lfs *LocalFileSource) AlreadyExists(dt, name string) bool {
	key := migration.CreateKeyFromVersionAndName(dt, name)
	filename := filepath.Join(lfs.folder, key +defaultMigrateFileFullExtension)
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
		versionRegexFormat = anyBasedVersionFormat
		nameRegexFormat = anyBasedNameFormat
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

func (lfs *LocalFileSource) Select(ctx context.Context, f Filter) (migration.Migrations, error) {
	keys, err := lfs.getAllVersionsFromFolder(f)
	if err != nil {
		return nil, err
	}

	migrationsCh := make(chan *migration.Migration)
	errorsCh := make(chan error)
	var wg sync.WaitGroup

	for k := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			m, err := lfs.readOne(key)
			if err != nil {
				mErr := errors.Wrapf(err, "with key %s", key)
				lfs.lg.Error(mErr)
				errorsCh <- err
			}

			migrationsCh <- m
		}(k)
	}

	go func() {
		wg.Wait()
		close(migrationsCh)
		close(errorsCh)
	}()

	var result migration.Migrations

	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case m, ok := <-migrationsCh:
			if ok {
				if m == nil {
					panic("how can a migration be null")
				}

				result = append(result, m)
			} else {
				sort.Sort(result)
				return filterMigrations(result, f), nil
			}
		case err, ok := <-errorsCh:
			if ok {
				return nil, err
			}
		}
	}
}

func (lfs *LocalFileSource) getAllVersionsFromFolder(f Filter) (map[string]int, error) {
	var onlyVersions []string

	for _, v := range f.Versions {
		onlyVersions = append(onlyVersions, v.Value)
	}

	files, err := ioutil.ReadDir(lfs.folder)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read keys from folder %s", lfs.folder)
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

		if len(onlyVersions) > 0 && !keyContainsOfVersions(key, onlyVersions) {
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

func (lfs *LocalFileSource) readOne(key string) (*migration.Migration, error) {
	up := filepath.Join(lfs.folder, key+defaultMigrateFileFullExtension)
	down := filepath.Join(lfs.folder, key+defaultRollbackFileFullExtension)

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

	return lfs.createMigration(key, migrateContents, rollbackContents)
}

func (lfs *LocalFileSource) createMigration(
	key string,
	migrateContents,
	rollbackContents []byte,
) (*migration.Migration, error) {
	name := lfs.extractNameFromKey(key)
	version, err := lfs.extractVersionFromKey(key)
	if err != nil {
		return nil, err
	}

	factory := migration.NewMigrationFromFile(key, name, version, string(migrateContents), string(rollbackContents))

	m, err := factory()
	if err != nil {
		return nil, err
	}
	return m, err
}

func (lfs *LocalFileSource) extractVersionFromKey(key string) (migration.Version, error) {
	var result migration.Version
	matches := lfs.versionRegexp.FindStringSubmatch(key)
	if len(matches) < 2 {
		return result, ErrInvalidTimestamp
	}

	result.Value = matches[1]

	return result, nil
}

func (lfs *LocalFileSource) extractNameFromKey(key string) string {
	matches := lfs.nameRegexp.FindStringSubmatch(key)
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