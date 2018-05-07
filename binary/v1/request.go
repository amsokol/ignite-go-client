package ignite

import (
	"bytes"
	"io"
)

// Request is interface of base message request functionality
type Request interface {
	// WriteTo is function to write request data to io.Writer.
	// Returns written bytes.
	WriteTo(w io.Writer) (int64, error)
}

// request is struct is implementing base message request functionality
type request struct {
	payload *bytes.Buffer

	Request
	io.Writer
}

// WriteTo is function to write request data to io.Writer.
// Returns written bytes.
func (r *request) WriteTo(w io.Writer) (int64, error) {
	return r.payload.WriteTo(w)
}

// Write writes len(p) bytes from p to the underlying data stream.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
// Write must return a non-nil error if it returns n < len(p).
// Write must not modify the slice data, even temporarily.
//
// Implementations must not retain p.
func (r *request) Write(p []byte) (n int, err error) {
	return r.payload.Write(p)
}

// newRequest is private constructor for request
func newRequest() request {
	return request{payload: &bytes.Buffer{}}
}
