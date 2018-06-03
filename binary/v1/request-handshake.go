package ignite

import (
	"encoding/binary"
	"io"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// RequestHandshake is struct handshake request
type RequestHandshake struct {
	major, minor, patch int
	username, password  string

	request
}

// WriteTo is function to write handshake request data to io.Writer.
// Returns written bytes.
func (r *RequestHandshake) WriteTo(w io.Writer) (int64, error) {
	if err := WriteByte(r, 1); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake code")
	}
	if err := WriteShort(r, int16(r.major)); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake version major")
	}
	if err := WriteShort(r, int16(r.minor)); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake version minor")
	}
	if err := WriteShort(r, int16(r.patch)); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake version patch")
	}
	if err := WriteByte(r, 2); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake client code")
	}
	if len(r.username) > 0 {
		if err := WriteOString(r, r.username); err != nil {
			return 0, errors.Wrapf(err, "failed to write handshake username")
		}
		if err := WriteOString(r, r.password); err != nil {
			return 0, errors.Wrapf(err, "failed to write handshake password")
		}
	}

	// write payload length
	l := int32(r.payload.Len())
	if err := binary.Write(w, binary.LittleEndian, &l); err != nil {
		return 0, errors.Wrapf(err, "failed to write handshake request length")
	}
	// write request
	n, err := r.request.WriteTo(w)
	return 4 + n, err
}

// NewRequestHandshake creates new handshake request object
func NewRequestHandshake(major, minor, patch int, username, password string) *RequestHandshake {
	return &RequestHandshake{request: newRequest(),
		major: major, minor: minor, patch: patch, username: username, password: password}
}
