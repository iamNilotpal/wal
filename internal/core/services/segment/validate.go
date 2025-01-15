package segment

import (
	"fmt"

	"github.com/iamNilotpal/wal/internal/core/domain"
)

// It ensures that all options are within acceptable ranges and
// that there are no conflicts between related options.
// Returns an error with a descriptive message if validation fails.
func Validate(opts *domain.SegmentOptions) error {
	if opts.MaxSegmentSize < opts.MinSegmentSize {
		return fmt.Errorf(
			"maxSegmentSize (%d) must be greater than minSegmentSize (%d)", opts.MaxSegmentSize, opts.MinSegmentSize,
		)
	}

	return nil
}
