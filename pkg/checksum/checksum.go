package checksum

import "hash/crc32"

func Checksum(data []byte) uint32 {
	table := crc32.MakeTable(crc32.IEEE)
	return crc32.Checksum(data, table)
}

func VerifyChecksum(data []byte, checksum uint32) bool {
	table := crc32.MakeTable(crc32.IEEE)
	newChecksum := crc32.Checksum(data, table)
	return newChecksum == checksum
}
