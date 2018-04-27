package ignite

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// RequestHandshake is struct handshake request
type RequestHandshake struct {
	major, minor, patch int

	request
}

// WriteTo is function to write handshake request data to io.Writer.
// Returns written bytes.
func (r *RequestHandshake) WriteTo(w io.Writer) (int64, error) {
	if err := r.WriteByte(1); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake code")
	}
	if err := r.WriteShort(int16(r.major)); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake version major")
	}
	if err := r.WriteShort(int16(r.minor)); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake version minor")
	}
	if err := r.WriteShort(int16(r.patch)); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake version patch")
	}
	if err := r.WriteByte(2); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake client code")
	}

	// write payload length
	l := int32(r.payload.Len())
	if err := binary.Write(w, binary.LittleEndian, &l); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake request length")
	}
	// write payload
	n, err := r.payload.WriteTo(w)
	return n + 4, err
}

// NewRequestHandshake creates new handshake request object
func NewRequestHandshake(major, minor, patch int) *RequestHandshake {
	return &RequestHandshake{request: request{payload: &bytes.Buffer{}},
		major: major, minor: minor, patch: patch}
}
