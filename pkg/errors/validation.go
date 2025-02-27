package errors

import "errors"

// ValidationError represents an error that occurs due to invalid input.
// It includes the field name, the invalid value, and the underlying error message.
type ValidationError struct {
	Value any    `json:"value"` // The actual value that failed validation.
	Field string `json:"field"` // Name of the field that caused the validation error.
	Err   error  `json:"error"` // The underlying error providing details about the validation issue.
}

// NewValidationError creates a new ValidationError instance.
func NewValidationError(field string, value any, err error) *ValidationError {
	return &ValidationError{
		Err:   err,
		Field: field,
		Value: value,
	}
}

// Error implements the error interface for ValidationError.
// It returns the error message associated with the validation failure.
func (e *ValidationError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "validation error"
}

// IsValidationError checks if a given error is of type ValidationError.
func IsValidationError(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

// AsValidationError attempts to extract a ValidationError from a given error.
func AsValidationError(err error) *ValidationError {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return ve
	}
	return nil
}
