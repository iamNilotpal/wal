package fs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type FileSystem interface {
	CreateDir(dirPath string, permission os.FileMode, force bool) error
	DeleteDir(dirPath string) error
	CopyDir(srcPath, destPath string) error
	ReadDir(dirName string) ([]string, error)

	CreateFile(filePath string, force bool) (*os.File, error)
	WriteFile(filePath string, permission os.FileMode, contents []byte) error
	ReadFile(filePath string) ([]byte, error)
	DeleteFile(filePath string) error
	CopyFile(sourcePath, destPath string) error

	SearchFiles(sourceDir string, excludeDirs []string, searchFile string) ([]string, error)
	SearchFileExtensions(sourceDir string, excludeDirs []string, extension string) ([]string, error)

	Pwd() (string, error)
	Exists(filePath string) (bool, error)
	Cd(dir string) error
}

type LocalFileSystem struct{}

func NewLocalFileSystem() *LocalFileSystem {
	return &LocalFileSystem{}
}

// Creates a directory if not present. Returns non nil error if directory is already present and force flag is false.
func (lfs *LocalFileSystem) CreateDir(dirPath string, permission os.FileMode, force bool) error {
	stat, err := os.Stat(dirPath)
	if !force && !os.IsNotExist(err) {
		err = fmt.Errorf("error in creating directory %s", dirPath)
		return err
	}

	if !stat.IsDir() {
		return errors.New("existing path isn't a directory")
	}

	if err := os.MkdirAll(dirPath, permission); err != nil {
		return fmt.Errorf("error in creating all directories %s", dirPath)
	}

	return os.Chmod(dirPath, 0755)
}

// Deletes a directory.
func (lfs *LocalFileSystem) DeleteDir(path string) error {
	return os.RemoveAll(path)
}

// Copy contents of the source directory to destination directory.
func (lfs *LocalFileSystem) CopyDir(src, dest string) error {
	srcStat, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !srcStat.IsDir() {
		return fmt.Errorf("source path : %s is not a directory", src)
	}

	if err := os.MkdirAll(dest, srcStat.Mode()); err != nil {
		return err
	}

	if err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		destPath := filepath.Join(dest, path[len(src)+1:])
		if err := os.MkdirAll(filepath.Dir(destPath), os.ModePerm); err != nil {
			return err
		}

		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		destFile, err := os.Create(destPath)
		if err != nil {
			return err
		}
		defer destFile.Close()

		if _, err := io.Copy(destFile, srcFile); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Reads the directory and retrieves all the file names.
func (lfs *LocalFileSystem) ReadDir(dirName string) ([]string, error) {
	files, err := filepath.Glob(dirName)
	return files, err
}

// Creates a file.
func (lfs *LocalFileSystem) CreateFile(filePath string, force bool) (*os.File, error) {
	_, err := os.Stat(filePath)
	if !force && os.IsExist(err) {
		return nil, fmt.Errorf("error in getting file stat %s because of %v", filePath, err)
	}
	return os.Create(filePath)
}

// Writes to a file.
func (lfs *LocalFileSystem) WriteFile(filePath string, permission os.FileMode, contents []byte) error {
	return os.WriteFile(filePath, contents, permission)
}

// Deletes a file.
func (lfs *LocalFileSystem) DeleteFile(filePath string) error {
	return os.Remove(filePath)
}

// Copies a file from source to destination.
func (lfs *LocalFileSystem) CopyFile(sourcePath, destPath string) error {
	input, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}
	return os.WriteFile(destPath, input, 0644)
}

// Read file contents.
func (lfs *LocalFileSystem) ReadFile(filePath string) ([]byte, error) {
	contents, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return contents, err
}

// Search for files.
func (lfs *LocalFileSystem) SearchFiles(sourceDir string, excludeDirs []string, searchFile string) ([]string, error) {
	files := make([]string, 0)

	if err := filepath.WalkDir(sourceDir, fs.WalkDirFunc(func(path string, ds fs.DirEntry, err error) error {
		if !ds.IsDir() && !isAncestor(excludeDirs, path) && filepath.Base(path) == searchFile {
			files = append(files, path)
		}
		return nil
	})); err != nil {
		return nil, err
	}

	return files, nil
}

// Search files with matching extensions.
func (lfs *LocalFileSystem) SearchFileExtensions(sourceDir string, excludeDirs []string, extension string) ([]string, error) {
	files := make([]string, 0)

	if err := filepath.WalkDir(sourceDir, fs.WalkDirFunc(func(path string, ds fs.DirEntry, err error) error {
		if !ds.IsDir() && !isAncestor(excludeDirs, path) && filepath.Ext(path) == extension {
			files = append(files, path)
		}
		return nil
	})); err != nil {
		return nil, err
	}

	return files, nil
}

// Get present working directory.
func (lfs *LocalFileSystem) Pwd() (string, error) {
	return os.Getwd()
}

// Checks if a file exists or not.
func (lfs *LocalFileSystem) Exists(file string) (bool, error) {
	_, err := os.Stat(file)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// Cd into a directory.
func (lfs *LocalFileSystem) Cd(dir string) error {
	return os.Chdir(dir)
}

// isAncestor
func isAncestor(excludeDirs []string, path string) bool {
	for _, excludeDir := range excludeDirs {
		if strings.Contains(path, excludeDir) {
			return true
		}
	}
	return false
}
