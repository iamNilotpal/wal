package checksum

import (
	"hash/crc32"
)

type crc32IEEE struct {
	name  string
	table *crc32.Table
}

func NewCRC32IEEE() *crc32IEEE {
	return &crc32IEEE{
		name:  string(CRC32IEEE),
		table: crc32.MakeTable(crc32.IEEE),
	}
}

func (c *crc32IEEE) Calculate(data []byte) uint64 {
	return uint64(crc32.Checksum(data, c.table))
}

func (c *crc32IEEE) Verify(data []byte, expected uint64) bool {
	checksum := uint64(crc32.Checksum(data, c.table))
	return checksum == expected
}

func (c *crc32IEEE) Size() uint8 {
	return crc32.Size
}

func (c *crc32IEEE) Name() string {
	return c.name
}
