package checksum

import "hash/crc32"

type crcChecksum struct{}

func NewCRCChecksum() *crcChecksum {
	return &crcChecksum{}
}

func (c crcChecksum) Checksum(data []byte) uint32 {
	table := crc32.MakeTable(crc32.IEEE)
	return crc32.Checksum(data, table)
}

func (c crcChecksum) Verify(data []byte, checksum uint32) bool {
	table := crc32.MakeTable(crc32.IEEE)
	newChecksum := crc32.Checksum(data, table)
	return newChecksum == checksum
}
