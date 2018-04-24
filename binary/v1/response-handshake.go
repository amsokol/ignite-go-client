package ignite

import (
	"bytes"
	"io"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// ResponseHandshake is interface of handshake message response
type ResponseHandshake interface {
	Response

	// Success flag
	Success() bool
	// Server version major
	Major() int
	// Server version minor
	Minor() int
	// Server version patch
	Patch() int
	// Error message
	Message() string
}

// responseHandshake is struct of base response functionality
type responseHandshake struct {
	// Success flag
	success bool
	// Server version major, minor, patch
	major, minor, patch int
	// Error message
	message string

	response
}

// Success flag
func (r *responseHandshake) Success() bool {
	return r.success
}

// Server version major
func (r *responseHandshake) Major() int {
	return r.major
}

// Server version minor
func (r *responseHandshake) Minor() int {
	return r.minor
}

// Server version patch
func (r *responseHandshake) Patch() int {
	return r.patch
}

// Error message
func (r *responseHandshake) Message() string {
	return r.message
}

// NewResponseHandshake creates new handshake response object
func NewResponseHandshake(r io.Reader) (ResponseHandshake, error) {
	rr := &responseHandshake{response: response{message: &bytes.Buffer{}}}

	var err error

	if _, err = rr.ReadFrom(r); err != nil {
		return nil, errors.Wrapf(err, "failed to read message")
	}

	if _, err = rr.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read message length")
	}

	rr.success, err = rr.ReadBool()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read success flag")
	}

	if !rr.success {
		v, err := rr.ReadShort()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read server version major")
		}
		rr.major = int(v)

		v, err = rr.ReadShort()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read server version minor")
		}
		rr.minor = int(v)

		v, err = rr.ReadShort()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read server version patch")
		}
		rr.patch = int(v)

		rr.message, _, err = rr.ReadOString()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read error message")
		}
	}

	return rr, nil
}
