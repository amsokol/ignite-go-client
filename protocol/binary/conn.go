package binary

import (
	"net"

	"github.com/amsokol/ignite-go-client/protocol/binary/internal"

	"github.com/amsokol/go-errors"
)

// Connect to Apache Ignite cluster
func Connect(network, address string, major, minor, patch int) (net.Conn, error) {
	// Dial
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open connection")
	}

	// Do handshake
	err = internal.Handshake(conn, internal.Version{Major: major, Minor: minor, Patch: patch})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to do handshake")
	}

	return conn, nil
}

// Connect100 to Apache Ignite cluster with protocol version 1.0.0
func Connect100(network, address string) (net.Conn, error) {
	return Connect(network, address, 1, 0, 0)
}
