package ignite

import (
	"io"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// ResponseOperation is struct operation response
type ResponseOperation struct {
	// Request id
	UID int64
	// Status code (0 for success, otherwise error code)
	Status int32
	// Error message (present only when status is not 0)
	Message string

	response
}

// ReadFrom is function to read request data from io.Reader.
// Returns read bytes.
func (r *ResponseOperation) ReadFrom(rr io.Reader) (int64, error) {
	// read response
	n, err := r.response.ReadFrom(rr)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read operation response")
	}

	uid, err := r.ReadLong()
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read operation request id")
	}

	r.Status, err = r.ReadInt()
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read status code")
	}

	if r.Status != 0 {
		r.Message, _, err = r.ReadOString()
		if err != nil {
			return 0, errors.Wrapf(err, "failed to read error message")
		}
	}

	if uid != r.UID {
		return n, errors.Errorf("invalid request ID: got %d, but expected %d", uid, r.UID)
	}

	return n, nil
}

// NewResponseOperation is ResponseOperation constructor
func NewResponseOperation(uid int64) *ResponseOperation {
	return &ResponseOperation{UID: uid}
}
