package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func CreateFile(fileName string) (*os.File, error) {
	file, err := os.Create(fileName)
	return file, err
}

func GetLastSegmentId(fileNames []string, prefix string) (uint8, error) {
	var lastSegmentId uint8 = 1

	for _, name := range fileNames {
		_, segment := filepath.Split(name)

		id, err := strconv.Atoi(strings.TrimPrefix(segment, prefix))
		if err != nil {
			return 0, err
		}

		segmentId := uint8(id)
		if segmentId > lastSegmentId {
			lastSegmentId = segmentId
		}
	}

	return lastSegmentId, nil
}

func GenerateSegmentName(prefix string, id uint8) string {
	return fmt.Sprintf("%s%d", prefix, id)
}
