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

	// ReadInt reads "int" value
	ReadInt() (int32, error)
	// ReadOInt reads "int" object value
	ReadOInt() (int32, error)

	// ReadLong reads "long" value
	ReadLong() (int64, error)
	// ReadOLong reads "long" object value
	ReadOLong() (int64, error)

	// ReadFloat reads "float" value
	ReadFloat() (float32, error)
	// ReadOFloat reads "float" object value
	ReadOFloat() (float32, error)

	// ReadDouble reads "double" value
	ReadDouble() (float64, error)
	// ReadODouble reads "double" object value
	ReadODouble() (float64, error)

	// ReadChar reads "char" value
	ReadChar() (rune, error)
	// ReadOChar reads "char" object value
	ReadOChar() (Char, error)

	// ReadBool reads "bool" value
	ReadBool() (bool, error)
	// ReadOBool reads "bool" object value
	ReadOBool() (bool, error)

	// ReadOString reads "string" object value
	// String is marshaled as object in all cases.
	ReadOString() (string, bool, error)

	// ReadFrom is function to read request data from io.Reader.
	// Returns read bytes.
	ReadFrom(r io.Reader) (int64, error)
}

// response is struct is implementing base message response functionality
type response struct {
	message io.Reader

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

// ReadLong reads "long" value
func (r *response) ReadLong() (int64, error) {
	var v int64
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadOLong reads "Long" object value
func (r *response) ReadOLong() (int64, error) {
	t, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if t != typeLong {
		return 0, errors.Errorf(errInvalidType, typeLong, t)
	}
	return r.ReadLong()
}

// ReadFloat reads "float" value
func (r *response) ReadFloat() (float32, error) {
	var v float32
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadOFloat reads "float" object value
func (r *response) ReadOFloat() (float32, error) {
	t, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if t != typeFloat {
		return 0, errors.Errorf(errInvalidType, typeFloat, t)
	}
	return r.ReadFloat()
}

// ReadDouble reads "Double" value
func (r *response) ReadDouble() (float64, error) {
	var v float64
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadODouble reads "Double" object value
func (r *response) ReadODouble() (float64, error) {
	t, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if t != typeDouble {
		return 0, errors.Errorf(errInvalidType, typeDouble, t)
	}
	return r.ReadDouble()
}

// ReadChar reads "char" value
func (r *response) ReadChar() (rune, error) {
	var v int16
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return rune(v), err
}

// ReadOChar reads "char" object value
func (r *response) ReadOChar() (Char, error) {
	t, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if t != typeChar {
		return 0, errors.Errorf(errInvalidType, typeChar, t)
	}
	c, err := r.ReadChar()
	return Char(c), err
}

// ReadBool reads "bool" value
func (r *response) ReadBool() (bool, error) {
	v, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	switch v {
	case 1:
		return true, nil
	case 0:
		return false, nil
	default:
		return false, errors.Errorf("invalid bool value: %d", v)
	}
}

// ReadOBool reads "bool" object value
func (r *response) ReadOBool() (bool, error) {
	t, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	if t != typeBool {
		return false, errors.Errorf(errInvalidType, typeBool, t)
	}
	return r.ReadBool()
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

// ReadFrom is function to read request data from io.Reader.
// Returns read bytes.
func (r *response) ReadFrom(rr io.Reader) (int64, error) {
	// read response length
	var l int32
	if err := binary.Read(rr, binary.LittleEndian, &l); err != nil {
		return 0, errors.Wrapf(err, "failed to read response length")
	}

	// read response message
	b := make([]byte, int(l))
	if err := binary.Read(rr, binary.LittleEndian, &b); err != nil {
		return 0, errors.Wrapf(err, "failed to read response data")
	}
	r.message = bytes.NewReader(b)

	return 4 + int64(l), nil
}
