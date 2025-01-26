package segment

import (
	"fmt"

	"github.com/iamNilotpal/wal/internal/core/domain"
	"github.com/iamNilotpal/wal/pkg/errors"
)

// It ensures that all options are within acceptable ranges and
// that there are no conflicts between related options.
// Returns an error with a descriptive message if validation fails.
func Validate(opts *domain.SegmentOptions) error {
	if opts.MaxSegmentSize < opts.MinSegmentSize {
		return errors.NewValidationError(
			"maxSegmentSize",
			opts.MaxSegmentSize,
			fmt.Errorf(
				"MaxSegmentSize (%d) must be greater than MinSegmentSize (%d)", opts.MaxSegmentSize, opts.MinSegmentSize,
			),
		)
	}

	return nil
}
