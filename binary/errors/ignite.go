package errors

import (
	"fmt"
)

const (
	// StatusSuccess means success
	StatusSuccess = 0
)

// IgniteError is Apache Ignite error
type IgniteError struct {
	// Apache Ignite specific status and message
	IgniteStatus  int32
	IgniteMessage string

	// error
	message string

	error
	fmt.Stringer
}

func (e *IgniteError) Error() string {
	return e.message
}

// Stringer is implemented by any value that has a String method,
// which defines the ``native'' format for that value.
// The String method is used to print values passed as an operand
// to any format that accepts a string or to an unformatted printer
// such as Print.
func (e *IgniteError) String() string {
	return e.Error()
}

// Errorf formats error
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

// NewError return error from Apache Ignite status and message
func NewError(status int32, message string) error {
	return &IgniteError{IgniteStatus: status, IgniteMessage: message,
		message: fmt.Sprintf("[%d] %s", status, message)}
}

// Wrapf formats error
func Wrapf(err error, format string, a ...interface{}) error {
	m := fmt.Sprintf("%s: %s", fmt.Sprintf(format, a...), err.Error())
	original, ok := err.(*IgniteError)
	if ok {
		original.message = m
		return original
	}
	return Errorf(m)
}
