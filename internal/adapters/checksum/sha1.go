package checksum

import (
	sha1_lib "crypto/sha1"
	"encoding/binary"
)

type sha1 struct {
	name string
}

func NewSHA1() *sha1 {
	return &sha1{name: string(SHA1)}
}

func (s *sha1) Calculate(data []byte) uint64 {
	h := sha1_lib.New()
	h.Write(data)

	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

func (s *sha1) Verify(data []byte, expected uint64) bool {
	return s.Calculate(data) == expected
}

func (s *sha1) Size() uint8 {
	return sha1_lib.Size
}

func (s *sha1) Name() string {
	return s.name
}
