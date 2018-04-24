package ignite

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

const (
	errInvalidType = "invalid type (expected %d, but got %d)"
)

// Response is interface of base message response functionality
type Response interface {
	// ReadByte reads "byte" value
	ReadByte() (byte, error)
	// ReadOByte reads "byte" object value
	ReadOByte() (byte, error)

	// ReadShort reads "short" value
	ReadShort() (int16, error)
	// ReadOShort reads "short" object value
	ReadOShort() (int16, error)

	// ReadInt reads "int32" value
	ReadInt() (int32, error)
	// ReadOInt reads "int" object value
	ReadOInt() (int32, error)

	// ReadOString reads "string" object value
	// String is marshaled as object in all cases.
	ReadOString() (string, error)

	// ReadFrom is abstract function to read request data from io.Reader.
	// Each child struct have to implement this function.
	// Returns written bytes.
	ReadFrom(r io.Reader) (int64, error)
}

// response is abstract struct is implementing base message response functionality
type response struct {
	message *bytes.Buffer

	Response
}

// ReadByte reads "byte" value
func (r *response) ReadByte() (byte, error) {
	var v byte
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadOByte reads "byte" object value
func (r *response) ReadOByte() (byte, error) {
	t, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if t != typeByte {
		return 0, errors.Errorf(errInvalidType, typeByte, t)
	}
	return r.ReadByte()
}

// ReadShort reads "short" value
func (r *response) ReadShort() (int16, error) {
	var v int16
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadOShort reads "short" object value
func (r *response) ReadOShort() (int16, error) {
	t, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if t != typeShort {
		return 0, errors.Errorf(errInvalidType, typeShort, t)
	}
	return r.ReadShort()
}

// ReadInt reads "int" value
func (r *response) ReadInt() (int32, error) {
	var v int32
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadOInt reads "int" object value
func (r *response) ReadOInt() (int32, error) {
	t, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if t != typeInt {
		return 0, errors.Errorf(errInvalidType, typeInt, t)
	}
	return r.ReadInt()
}

// ReadOString reads "string" object value
// String is marshaled as object in all cases.
func (r *response) ReadOString() (string, bool, error) {
	t, err := r.ReadByte()
	if err != nil {
		return "", false, err
	}
	switch t {
	case typeNULL:
		return "", true, nil
	case typeString:
		l, err := r.ReadInt()
		if err != nil {
			return "", false, err
		}
		if l > 0 {
			s := make([]byte, l)
			if err = binary.Read(r.message, binary.LittleEndian, &s); err != nil {
				return "", false, err
			}
			return string(s), false, nil
		}
		return "", false, nil
	default:
		return "", false, errors.Errorf(errInvalidType, typeString, t)
	}
}
