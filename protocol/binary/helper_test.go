package binary

import (
	"net"

	"github.com/amsokol/ignite-go-client/protocol/binary/internal"

	"github.com/amsokol/go-errors"
)

func GetTestConnection() (net.Conn, error) {
	// Dial
	conn, err := net.Dial("tcp", "127.0.0.1:10800")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open connection")
	}

	// Do handshake
	err = internal.Handshake(conn, internal.Version{Major: 1, Minor: 0, Patch: 0})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to do handshake")
	}

	return conn, nil
}
