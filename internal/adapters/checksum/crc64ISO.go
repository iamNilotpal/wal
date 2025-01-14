package checksum

import (
	"hash/crc64"
)

type crc64ISO struct {
	name  string
	table *crc64.Table
}

func NewCRC64ISO() *crc64ISO {
	return &crc64ISO{
		name:  string(CRC64ISO),
		table: crc64.MakeTable(crc64.ISO),
	}
}

func (c *crc64ISO) Calculate(data []byte) uint64 {
	return crc64.Checksum(data, c.table)
}

func (c *crc64ISO) Verify(data []byte, expected uint64) bool {
	checksum := uint64(crc64.Checksum(data, c.table))
	return checksum == expected
}

func (c *crc64ISO) Size() uint16 {
	return crc64.Size
}

func (c *crc64ISO) Name() string {
	return c.name
}
