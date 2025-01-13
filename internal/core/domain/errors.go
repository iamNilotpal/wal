package domain

// ErrorCategory classifies different types of errors
type ErrorCategory int

const (
	ErrorStorage ErrorCategory = iota
	ErrorCompression
	ErrorCheckpoint
	ErrorRecovery
	ErrorBatch
)
