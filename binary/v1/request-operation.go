package ignite

import (
	"bytes"
	"math/rand"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// RequestOperation is struct to store operation request
type RequestOperation struct {
	UID int64

	request
}

// NewRequestOperation creates new handshake request object
func NewRequestOperation(code int) (*RequestOperation, error) {
	r := &RequestOperation{request: request{payload: &bytes.Buffer{}}, UID: rand.Int63()}

	if err := r.WriteShort(int16(code)); err != nil {
		return nil, errors.Wrapf(err, "failed to write operation code")
	}

	if err := r.WriteLong(r.UID); err != nil {
		return nil, errors.Wrapf(err, "failed to write request id")
	}

	return r, nil
}
