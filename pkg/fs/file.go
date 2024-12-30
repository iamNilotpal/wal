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

func GetLatestSegmentId(fileNames []string, prefix string) (uint64, error) {
	var lastSegmentId uint64 = 0

	for _, name := range fileNames {
		_, segment := filepath.Split(name)

		id, err := strconv.Atoi(strings.TrimPrefix(segment, prefix))
		if err != nil {
			return 0, err
		}

		segmentId := uint64(id)
		if segmentId > lastSegmentId {
			lastSegmentId = segmentId
		}
	}

	return lastSegmentId, nil
}

func GenerateSegmentName(prefix string, id uint64) string {
	return fmt.Sprintf("%s%d", prefix, id)
}
