package errors

import "errors"

type validationError struct {
	Value any    `json:"value"`
	Field string `json:"field"`
	Err   error  `json:"error"`
}

func NewValidationError(field string, value any, err error) *validationError {
	return &validationError{Value: value, Field: field, Err: err}
}

func (e *validationError) Error() string {
	return e.Err.Error()
}

func IsValidationError(err error) bool {
	var ve *validationError
	return errors.As(err, &ve)
}

func GetValidationError(err error) *validationError {
	var ve *validationError
	if !errors.As(err, &ve) {
		return nil
	}
	return ve
}
