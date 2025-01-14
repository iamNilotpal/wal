package checksum

import (
	sha256_lib "crypto/sha256"
	"encoding/binary"

	"github.com/iamNilotpal/wal/internal/core/domain"
)

type sha256 struct {
	name string
}

func NewSHA256() *sha256 {
	return &sha256{name: string(domain.SHA256)}
}

func (s *sha256) Calculate(data []byte) uint64 {
	h := sha256_lib.New()
	h.Write(data)

	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

func (s *sha256) Verify(data []byte, expected uint64) bool {
	return s.Calculate(data) == expected
}

func (s *sha256) Size() uint8 {
	return sha256_lib.Size
}

func (s *sha256) Name() string {
	return s.name
}
