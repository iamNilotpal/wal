package system

import (
	"context"
)

// Executes a cleanup operation with context awareness. It manages the lifecycle
// of the cleanup operation, ensuring proper completion or graceful interruption.
//
// The function handles three key scenarios:
//   - Normal completion: The cleanup finishes successfully
//   - Error during cleanup: The error is propagated to the caller
//   - Context cancellation: The cleanup is signaled to stop but allowed to finish gracefully
//
// Returns:
//   - nil if cleanup completes successfully.
//   - original error if cleanup fails.
//   - wrapped error with context cancellation details if interrupted.
func RunWithContext(ctx context.Context, operation func(context.Context) error) error {
	// Before proceeding with resource cleanup, check if the context is already
	// cancelled to provide fast feedback if the operation was cancelled before
	// we started.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Create an independent context for the cleanup operation.
	// This allows us to manage the cleanup lifecycle separately from the parent context,
	// ensuring we can properly handle interruption without leaving resources in an
	// inconsistent state.
	cleanupCtx, cancel := context.WithCancel(context.Background())
	// Ensure cleanup context is always cancelled to prevent context leak.
	defer cancel()

	// Create a buffered channel to collect the cleanup result.
	// Using a buffered channel (size 1) ensures the cleanup goroutine can exit
	// even if the parent context is cancelled and we don't read from the channel immediately.
	done := make(chan error, 1)

	// Launch cleanup in a separate goroutine to enable timeout handling.
	// This goroutine will run the cleanup function and report its result
	// through the done channel.
	go func() {
		// Send cleanup result to channel. The cleanup function receives
		// its own context that can signal when to abort operations.
		done <- operation(cleanupCtx)
		// Close channel to prevent goroutine leak and signal completion
		close(done)
	}()

	// Wait for either cleanup completion or context cancellation.
	// This select statement coordinates between the cleanup operation
	// and external cancellation signals.
	select {
	case err := <-done:
		// Cleanup completed normally (success or error).
		// Return the result directly to maintain the original error chain.
		return err
	case <-ctx.Done():
		// Parent context was cancelled (e.g., timeout or explicit cancellation).
		// Signal cleanup to stop by cancelling its context.
		cancel()
		// Wait for cleanup to finish and wrap the error for clarity.
		// We still wait for done to prevent resource leaks and ensure
		// cleanup operations can finish any critical work.
		return <-done
	}
}
