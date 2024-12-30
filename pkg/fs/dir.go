package fs

import (
	"errors"
	"os"
	"path/filepath"
)

func CreateDir(dirName string) error {
	err := os.Mkdir(dirName, os.ModeExclusive)
	if err == nil {
		return nil
	}

	if os.IsExist(err) {
		stat, err := os.Stat(dirName)
		if err != nil {
			return err
		}

		if !stat.IsDir() {
			return errors.New("existing path isn't a directory")
		}
		return nil
	}

	return err
}

func ReadFileNames(dirName string) ([]string, error) {
	files, err := filepath.Glob(dirName)
	return files, err
}
