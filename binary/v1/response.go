package ignite

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// Response is interface of base message response functionality
type Response interface {
	// ReadFrom is function to read request data from io.Reader.
	// Returns read bytes.
	ReadFrom(r io.Reader) (int64, error)
}

// response is struct is implementing base message response functionality
type response struct {
	message io.Reader

	Response
	io.Reader
}

// ReadFrom is function to read request data from io.Reader.
// Returns read bytes.
func (r *response) ReadFrom(rr io.Reader) (int64, error) {
	// read response length
	var l int32
	if err := binary.Read(rr, binary.LittleEndian, &l); err != nil {
		return 0, errors.Wrapf(err, "failed to read response length")
	}

	// read response message
	b := make([]byte, int(l))
	if err := binary.Read(rr, binary.LittleEndian, &b); err != nil {
		return 0, errors.Wrapf(err, "failed to read response data")
	}
	r.message = bytes.NewReader(b)

	return 4 + int64(l), nil
}

// Read reads up to len(p) bytes into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered. Even if Read
// returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, Read conventionally
// returns what is available instead of waiting for more.
func (r *response) Read(p []byte) (n int, err error) {
	return r.message.Read(p)
}
