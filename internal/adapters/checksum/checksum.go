package checksum

import (
	"fmt"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/internal/core/ports"
	"github.com/iamNilotpal/wal/pkg/errors"
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

func NewCheckSummer(alg domain.ChecksumAlgorithm) ports.ChecksumPort {
	switch alg {
	case CRC32IEEE:
		return NewCRC32IEEE()
	case CRC64ISO:
		return NewCRC64ISO()
	case CRC64ECMA:
		return NewCR64ECMA()
	case SHA1:
		return NewSHA1()
	case SHA256:
		return NewSHA256()
	default:
		return NewCRC32IEEE()
	}
}

// Returns recommended checksum settings.
func DefaultOptions() *domain.ChecksumOptions {
	return &domain.ChecksumOptions{
		Enable:        true,
		VerifyOnRead:  true,
		VerifyOnWrite: true,
		Algorithm:     CRC32IEEE,
	}
}

func Validate(opts *domain.ChecksumOptions) error {
	if opts == nil {
		return nil
	}

	if opts.Custom == nil {
		switch opts.Algorithm {
		case CRC32IEEE, CRC64ISO, CRC64ECMA, SHA1, SHA256:
		default:
			return errors.NewValidationError(
				"Algorithm", opts.Algorithm, fmt.Errorf("unsupported checksum algorithm: %s", opts.Algorithm),
			)
		}
	}

	return nil
}
