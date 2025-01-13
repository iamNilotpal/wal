package ports

import "os"

// Defines an interface for performing file and directory operations.
// It provides methods for creating, reading, writing, copying, and deleting
// files and directories, as well as search functionality and directory navigation.
type FileSystemPort interface {
	// Creates a new directory at the specified path with given permissions.
	// If force is true, it will create all necessary parent directories.
	// Returns an error if the operation fails.
	CreateDir(dirPath string, permission os.FileMode, force bool) error

	// Removes the directory at the specified path and all its contents.
	// Returns an error if the directory doesn't exist or the operation fails.
	DeleteDir(dirPath string) error

	// Recursively copies a directory from srcPath to destPath.
	// Includes all contents and preserves file modes.
	// Returns an error if either path is invalid or the operation fails.
	CopyDir(srcPath, destPath string) error

	// Returns a slice of names of all entries in the specified directory.
	// Returns an error if the directory cannot be read.
	ReadDir(dirName string) ([]string, error)

	// Creates a new file at the specified path.
	// If force is true, it will create any necessary parent directories.
	// Returns a pointer to the created file and any error encountered.
	CreateFile(filePath string, force bool) (*os.File, error)

	// Writes the given contents to a file at filePath with specified permissions.
	// Returns an error if the write operation fails.
	WriteFile(filePath string, permission os.FileMode, contents []byte) error

	// Reads the entire contents of the file at filePath.
	// Returns the contents as a byte slice and any error encountered.
	ReadFile(filePath string) ([]byte, error)

	// Removes the specified file.
	// Returns an error if the file doesn't exist or cannot be deleted.
	DeleteFile(filePath string) error

	// Copies a file from sourcePath to destPath. Preserves file modes and contents.
	// Returns an error if either path is invalid or the operation fails.
	CopyFile(sourcePath, destPath string) error

	// Searches for files with exact name match in sourceDir.
	// Directories in excludeDirs are skipped during the search.
	// Returns paths of matching files and any error encountered.
	SearchFiles(sourceDir string, excludeDirs []string, searchFile string) ([]string, error)

	// Searches for files with the specified extension in sourceDir.
	// Directories in excludeDirs are skipped during the search.
	// Returns paths of matching files and any error encountered.
	SearchFileExtensions(sourceDir string, excludeDirs []string, extension string) ([]string, error)

	// Returns the current working directory path.
	// Returns an error if the current directory cannot be determined.
	Pwd() (string, error)

	// Checks if a file or directory exists at the specified path.
	// Returns true if the path exists, false if it doesn't, and any error encountered.
	Exists(filePath string) (bool, error)

	// Changes the current working directory to the specified directory.
	// Returns an error if the directory doesn't exist or cannot be accessed.
	Cd(dir string) error
}
