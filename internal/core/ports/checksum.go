package ports

type Checksum interface {
	Checksum(data []byte) uint32
	VerifyChecksum(data []byte, checksum uint32) bool
}
