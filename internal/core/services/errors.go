package services

import (
	"fmt"
	"time"

	"github.com/iamNilotpal/wal/internal/core/domain"
)

type WALError struct {
	IsRetryAble bool
	Err         error
	Operation   string
	Timestamp   time.Time
	Category    domain.ErrorCategory
}

func (e *WALError) Error() string {
	return fmt.Sprintf("[%v] %s: %v : %s", e.Category, e.Operation, e.Err, e.Timestamp.String())
}
