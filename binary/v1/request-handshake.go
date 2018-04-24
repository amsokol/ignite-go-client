package ignite

import (
	"bytes"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// requestHandshake is struct of base request functionality
type requestHandshake struct {
	request
}

// NewRequestHandshake creates new handshake request object
func NewRequestHandshake(major, minor, patch int) (Request, error) {
	r := &requestHandshake{request: request{payload: &bytes.Buffer{}}}

	if err := r.WriteInt(8); err != nil {
		return nil, errors.Wrapf(err, "failed to write handshake request payload length")
	}
	if err := r.WriteByte(1); err != nil {
		return nil, errors.Wrapf(err, "failed to write handshake code")
	}
	if err := r.WriteShort(int16(major)); err != nil {
		return nil, errors.Wrapf(err, "failed to write handshake version major")
	}
	if err := r.WriteShort(int16(minor)); err != nil {
		return nil, errors.Wrapf(err, "failed to write handshake version minor")
	}
	if err := r.WriteShort(int16(patch)); err != nil {
		return nil, errors.Wrapf(err, "failed to write handshake version patch")
	}
	if err := r.WriteByte(2); err != nil {
		return nil, errors.Wrapf(err, "failed to write handshake client code")
	}

	return r, nil
}
