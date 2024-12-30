package fs

import (
	"errors"
	"os"
)

func MustCreateDir(dirName string) error {
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
			return errors.New("path exists but is not a directory")
		}
		return nil
	}

	return err
}
