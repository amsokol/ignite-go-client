package internal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/amsokol/go-errors"
)

// Handshake - besides socket connection, the thin client protocol requires a connection handshake to ensure
// that client and server versions are compatible. Note that handshake must be the first message
// after connection establishment.
func Handshake(rw io.ReadWriter, v Version) error {
	// Message length
	if err := binary.Write(rw, binary.LittleEndian, int32(8)); err != nil {
		return errors.Wrapf(err, "failed to write handshake message length")
	}

	// Handshake operation
	if err := binary.Write(rw, binary.LittleEndian, byte(1)); err != nil {
		return errors.Wrapf(err, "failed to write handshake operation")
	}

	// Protocol version 1.0.0
	if err := binary.Write(rw, binary.LittleEndian, int16(v.Major)); err != nil {
		return errors.Wrapf(err, "failed to write handshake protocol version (major)")
	}
	if err := binary.Write(rw, binary.LittleEndian, int16(v.Minor)); err != nil {
		return errors.Wrapf(err, "failed to write handshake protocol version (minor)")
	}
	if err := binary.Write(rw, binary.LittleEndian, int16(v.Patch)); err != nil {
		return errors.Wrapf(err, "failed to write handshake protocol version (patch)")
	}

	// Client code
	if err := binary.Write(rw, binary.LittleEndian, byte(2)); err != nil {
		return errors.Wrapf(err, "failed to write handshake client code")
	}

	// Read handshake response
	var len int32
	if err := binary.Read(rw, binary.LittleEndian, &len); err != nil {
		return errors.Wrapf(err, "failed to read handshake response length")
	}
	var res byte
	if err := binary.Read(rw, binary.LittleEndian, &res); err != nil {
		return errors.Wrapf(err, "failed to read handshake response result")
	}

	if res != 1 {
		var major, minor, patch int16
		if err := binary.Read(rw, binary.LittleEndian, &major); err != nil {
			return errors.Wrapf(err, "failed to read handshake error protocol version (major)")
		}
		if err := binary.Read(rw, binary.LittleEndian, &minor); err != nil {
			return errors.Wrapf(err, "failed to read handshake error protocol version (minor)")
		}
		if err := binary.Read(rw, binary.LittleEndian, &patch); err != nil {
			return errors.Wrapf(err, "failed to read handshake error protocol version (patch)")
		}
		ver := fmt.Sprintf("%d.%d.%d", major, minor, patch)

		var s String
		if err := s.Read(rw); err != nil {
			return errors.Wrapf(err, "failed to read handshake error reason")
		}
		return errors.Newff(errors.Fields{"reason": s.Value(), "server-protocol": ver}, "handshake failed")
	}

	return nil
}
