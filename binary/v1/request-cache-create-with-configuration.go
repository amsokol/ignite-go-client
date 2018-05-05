package ignite

import (
	"encoding/binary"
	"io"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// RequestCacheCreateWithConfiguration is struct to store operation request
type RequestCacheCreateWithConfiguration struct {
	Count int16

	RequestOperation
}

// WriteTo is function to write handshake request data to io.Writer.
// Returns written bytes.
func (r *RequestCacheCreateWithConfiguration) WriteTo(w io.Writer) (int64, error) {
	// write payload length
	l := int32(2 + 8 + 4 + 2 + r.payload.Len())
	if err := binary.Write(w, binary.LittleEndian, &l); err != nil {
		return 0, errors.Wrapf(err, "failed to write operation request length")
	}

	// write operation code
	if err := binary.Write(w, binary.LittleEndian, &r.Code); err != nil {
		return 0, errors.Wrapf(err, "failed to write operation code")
	}

	// write operation request id
	if err := binary.Write(w, binary.LittleEndian, &r.UID); err != nil {
		return 0, errors.Wrapf(err, "failed to write operation request id")
	}

	// write data length
	l = int32(r.payload.Len())
	if err := binary.Write(w, binary.LittleEndian, &l); err != nil {
		return 0, errors.Wrapf(err, "failed to write data length")
	}

	// write params count
	if err := binary.Write(w, binary.LittleEndian, &r.Count); err != nil {
		return 0, errors.Wrapf(err, "failed to write params count")
	}

	// write payload
	n, err := r.payload.WriteTo(w)
	return 4 + 2 + 8 + 4 + 2 + n, err
}

// NewRequestCacheCreateWithConfiguration creates new handshake request object
func NewRequestCacheCreateWithConfiguration(code int16) *RequestCacheCreateWithConfiguration {
	return &RequestCacheCreateWithConfiguration{RequestOperation: *NewRequestOperation(code)}
}
