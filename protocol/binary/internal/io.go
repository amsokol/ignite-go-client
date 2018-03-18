package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/amsokol/go-errors"
)

const (
	requestHeaderSize = 10

	// StatusSuccess means success
	statusSuccess = 0
)

// ReadTypeCode reads tpe code from stream
func ReadTypeCode(r io.Reader) (int, error) {
	var code byte
	err := binary.Read(r, binary.LittleEndian, &code)
	return int(code), err
}

// Read reads message (header + data) from stream
func Read(r io.Reader, vid bool, id *int64, status *int, message *string, a ...Type) error {
	// Response length
	var size int32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return errors.Wrapf(err, "failed to read response length")
	}

	// Response id
	oid := *id
	if err := binary.Read(r, binary.LittleEndian, id); err != nil {
		return errors.Wrapf(err, "failed to read response id")
	}
	if vid && oid != *id {
		return errors.New(fmt.Sprintf("invalid operation id returned (got %d, but expected %d)", *id, oid))
	}

	// Response status
	var res int32
	if err := binary.Read(r, binary.LittleEndian, &res); err != nil {
		return errors.Wrapf(err, "failed to read response status")
	}
	*status = int(res)

	if *status != statusSuccess {
		// Response status
		var s String
		if err := s.Read(r); err != nil {
			return errors.Wrapf(err, "failed to read response error message")
		}
		*message = s.Value()
		return nil
	}

	for i, v := range a {
		if err := v.Read(r); err != nil {
			return errors.Wrapf(err, "failed to read value with index="+strconv.Itoa(i))
		}
	}
	return nil
}

// Write writes message (header + data) to stream
func Write(w io.Writer, code int, id int64, a ...Type) error {
	size := requestHeaderSize

	// Calculate data size
	for _, v := range a {
		size += v.Size()
	}

	// Message length
	if err := binary.Write(w, binary.LittleEndian, int32(size)); err != nil {
		return errors.Wrapf(err, "failed to write request length")
	}

	// Op code
	if err := binary.Write(w, binary.LittleEndian, int16(code)); err != nil {
		return errors.Wrapf(err, "failed to write operation code")
	}

	// Request id
	if err := binary.Write(w, binary.LittleEndian, id); err != nil {
		return errors.Wrapf(err, "failed to write request id")
	}

	// Write data
	for i, v := range a {
		if err := v.Write(w); err != nil {
			return errors.Wrapf(err, "failed to write value with index="+strconv.Itoa(i))
		}
	}
	return nil
}
