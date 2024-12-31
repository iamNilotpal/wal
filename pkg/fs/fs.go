package fs

import (
	"errors"
	"os"
	"path/filepath"
)

func CreateFile(fileName string) (*os.File, error) {
	file, err := os.Create(fileName)
	return file, err
}

// Try to create a directory. If something is already there with given name,
// then checks whether it's a directory or not. If not then returns a non nil error.
func CreateDir(dirName string) error {
	err := os.Mkdir(dirName, 0750)
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

// Reads the directory and retrieves all the file names.
func ReadDirectory(dirName string) ([]string, error) {
	files, err := filepath.Glob(dirName)
	return files, err
}
