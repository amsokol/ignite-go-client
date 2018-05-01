package ignite

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

const (
	errInvalidType = "invalid type (expected %d, but got %d)"
)

// Response is interface of base message response functionality
type Response interface {
	// ReadByte reads "byte" value
	ReadByte() (byte, error)

	// ReadShort reads "short" value
	ReadShort() (int16, error)

	// ReadInt reads "int" value
	ReadInt() (int32, error)

	// ReadLong reads "long" value
	ReadLong() (int64, error)

	// ReadFloat reads "float" value
	ReadFloat() (float32, error)

	// ReadDouble reads "double" value
	ReadDouble() (float64, error)

	// ReadChar reads "char" value
	ReadChar() (Char, error)

	// ReadBool reads "bool" value
	ReadBool() (bool, error)

	// ReadString reads "string" value
	ReadString() (string, error)
	// ReadOString reads "string" object value
	ReadOString() (string, bool, error)

	// ReadUUID reads "UUID" object value
	ReadUUID() (uuid.UUID, error)

	// ReadDate reads "Date" object value
	ReadDate() (time.Time, error)

	// ReadByteArray reads "byte" array value
	ReadByteArray() ([]byte, error)

	// ReadTimestamp reads "Timestamp" object value
	ReadTimestamp() (time.Time, error)

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

// ReadShort reads "short" value
func (r *response) ReadShort() (int16, error) {
	var v int16
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadInt reads "int" value
func (r *response) ReadInt() (int32, error) {
	var v int32
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadLong reads "long" value
func (r *response) ReadLong() (int64, error) {
	var v int64
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadFloat reads "float" value
func (r *response) ReadFloat() (float32, error) {
	var v float32
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadDouble reads "Double" value
func (r *response) ReadDouble() (float64, error) {
	var v float64
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return v, err
}

// ReadChar reads "char" value
func (r *response) ReadChar() (Char, error) {
	var v int16
	err := binary.Read(r.message, binary.LittleEndian, &v)
	return Char(v), err
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

// ReadString reads "string" value
func (r *response) ReadString() (string, error) {
	l, err := r.ReadInt()
	if err != nil {
		return "", err
	}
	if l > 0 {
		s := make([]byte, l)
		if err = binary.Read(r.message, binary.LittleEndian, &s); err != nil {
			return "", err
		}
		return string(s), nil
	}
	return "", nil
}

// ReadOString reads "string" object value
func (r *response) ReadOString() (string, bool, error) {
	t, err := r.ReadByte()
	if err != nil {
		return "", false, err
	}
	switch t {
	case typeNULL:
		return "", true, nil
	case typeString:
		v, err := r.ReadString()
		return v, false, err
	default:
		return "", false, errors.Errorf(errInvalidType, typeString, t)
	}
}

// ReadUUID reads "UUID" object value
func (r *response) ReadUUID() (uuid.UUID, error) {
	var o uuid.UUID
	err := binary.Read(r.message, binary.LittleEndian, &o)
	return o, err
}

// ReadDate reads "Date" object value
func (r *response) ReadDate() (time.Time, error) {
	v, err := r.ReadLong()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(v)/1000, (int64(v)%1000)*int64(time.Millisecond)).UTC(), nil
}

// ReadByteArray reads "byte" array value
func (r *response) ReadByteArray() ([]byte, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]byte, l)
	if l > 0 {
		err = binary.Read(r.message, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadTimestamp reads "Timestamp" object value
func (r *response) ReadTimestamp() (time.Time, error) {
	high, err := r.ReadLong()
	if err != nil {
		return time.Time{}, err
	}
	low, err := r.ReadInt()
	if err != nil {
		return time.Time{}, err
	}
	low = int32((high%1000)*int64(time.Millisecond)) + low
	high = high / 1000
	return time.Unix(high, int64(low)).UTC(), nil
}

func (r *response) ReadObject() (interface{}, error) {
	t, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch t {
	case typeByte:
		return r.ReadByte()
	case typeShort:
		return r.ReadShort()
	case typeInt:
		return r.ReadInt()
	case typeLong:
		return r.ReadLong()
	case typeFloat:
		return r.ReadFloat()
	case typeDouble:
		return r.ReadDouble()
	case typeChar:
		return r.ReadChar()
	case typeBool:
		return r.ReadBool()
	case typeString:
		return r.ReadString()
	case typeUUID:
		return r.ReadUUID()
	case typeDate:
		return r.ReadDate()
	case typeByteArray:
		return r.ReadByteArray()
	case typeTimestamp:
		return r.ReadTimestamp()
	case typeNULL:
		return nil, nil
	default:
		return nil, errors.Errorf("unsupported object type: %d", t)
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
