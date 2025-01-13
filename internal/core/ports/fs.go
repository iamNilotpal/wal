package ports

import "os"

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
