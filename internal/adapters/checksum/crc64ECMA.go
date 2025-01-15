package checksum

import (
	"hash/crc64"
)

type crc64ECMA struct {
	name  string
	table *crc64.Table
}

func NewCR64ECMA() *crc64ECMA {
	return &crc64ECMA{
		name:  string(CRC64ECMA),
		table: crc64.MakeTable(crc64.ECMA),
	}
}

func (c *crc64ECMA) Calculate(data []byte) uint64 {
	return crc64.Checksum(data, c.table)
}

func (c *crc64ECMA) Verify(data []byte, expected uint64) bool {
	checksum := uint64(crc64.Checksum(data, c.table))
	return checksum == expected
}

func (c *crc64ECMA) Size() uint8 {
	return crc64.Size
}

func (c *crc64ECMA) Name() string {
	return c.name
}
