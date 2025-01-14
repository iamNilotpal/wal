package checksum

import (
	"fmt"

	"github.com/iamNilotpal/wal/internal/core/domain"
)

const (
	// CRC32IEEE uses the IEEE polynomial for CRC32 checksums
	CRC32IEEE domain.ChecksumAlgorithm = "crc32-ieee"

	// CRC64ISO uses the ISO polynomial for CRC64 checksums
	CRC64ISO domain.ChecksumAlgorithm = "crc64-iso"

	// CRC64ECMA uses the ECMA polynomial for CRC64 checksums
	CRC64ECMA domain.ChecksumAlgorithm = "crc64-ecma"

	// SHA1 provides SHA-1 checksums (160-bit)
	SHA1 domain.ChecksumAlgorithm = "sha1"

	// SHA256 provides SHA-256 checksums (256-bit)
	SHA256 domain.ChecksumAlgorithm = "sha256"
)

// Returns recommended checksum settings.
func DefaultOptions() *domain.ChecksumOptions {
	return &domain.ChecksumOptions{
		Enable:        true,
		VerifyOnRead:  true,
		VerifyOnWrite: true,
		Algorithm:     CRC32IEEE,
	}
}

func Validate(input *domain.ChecksumOptions) error {
	if input.Custom == nil {
		switch input.Algorithm {
		case CRC32IEEE, CRC64ISO, CRC64ECMA, SHA1, SHA256:
		default:
			return fmt.Errorf("unsupported checksum algorithm: %s", input.Algorithm)
		}
	}
	return nil
}
