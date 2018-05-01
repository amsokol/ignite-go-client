package ignite

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"time"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// Request is interface of base message request functionality
type Request interface {
	// WriteByte writes "byte" value
	WriteByte(v byte) error
	// WriteOByte writes "byte" object value
	WriteOByte(v byte) error

	// WriteShort writes "short" value
	WriteShort(v int16) error
	// WriteOShort writes "short" object value
	WriteOShort(v int16) error

	// WriteInt writes "int" value
	WriteInt(v int32) error
	// WriteOInt writes "int" object value
	WriteOInt(v int32) error

	// WriteLong writes "long" value
	WriteLong(v int64) error
	// WriteOLong writes "long" object value
	WriteOLong(v int64) error

	// WriteFloat writes "float" value
	WriteFloat(v float32) error
	// WriteOFloat writes "float" object value
	WriteOFloat(v float32) error

	// WriteDouble writes "double" value
	WriteDouble(v float64) error
	// WriteODouble writes "double" object value
	WriteODouble(v float64) error

	// WriteChar writes "char" value
	WriteChar(v rune) error
	// WriteOChar writes "char" object value
	WriteOChar(v Char) error

	// WriteBool writes "bool" value
	WriteBool(v bool) error
	// WriteOBool writes "bool" object value
	WriteOBool(v bool) error

	// WriteOString writes "string" object value
	// String is marshaled as object in all cases.
	WriteOString(v string) error

	// WriteOTimestamp writes "Timestamp" object value
	// Timestamp is marshaled as object in all cases.
	WriteOTimestamp(v time.Time) error

	// WriteTo is function to write request data to io.Writer.
	// Returns written bytes.
	WriteTo(w io.Writer) (int64, error)
}

// request is struct is implementing base message request functionality
type request struct {
	payload *bytes.Buffer

	Request
}

// WriteByte writes "byte" value
func (r *request) WriteByte(v byte) error {
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOByte writes "byte" object value
func (r *request) WriteOByte(v byte) error {
	if err := r.WriteByte(typeByte); err != nil {
		return err
	}
	return r.WriteByte(v)
}

// WriteShort writes "short" value
func (r *request) WriteShort(v int16) error {
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOShort writes "short" object value
func (r *request) WriteOShort(v int16) error {
	if err := r.WriteByte(typeShort); err != nil {
		return err
	}
	return r.WriteShort(v)
}

// WriteInt writes "int" value
func (r *request) WriteInt(v int32) error {
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOInt writes "int" object value
func (r *request) WriteOInt(v int32) error {
	if err := r.WriteByte(typeInt); err != nil {
		return err
	}
	return r.WriteInt(v)
}

// WriteLong writes "long" value
func (r *request) WriteLong(v int64) error {
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOLong writes "long" object value
func (r *request) WriteOLong(v int64) error {
	if err := r.WriteByte(typeLong); err != nil {
		return err
	}
	return r.WriteLong(v)
}

// WriteFloat writes "float" value
func (r *request) WriteFloat(v float32) error {
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOFloat writes "float" object value
func (r *request) WriteOFloat(v float32) error {
	if err := r.WriteByte(typeFloat); err != nil {
		return err
	}
	return r.WriteFloat(v)
}

// WriteDouble writes "double" value
func (r *request) WriteDouble(v float64) error {
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteODouble writes "double" object value
func (r *request) WriteODouble(v float64) error {
	if err := r.WriteByte(typeDouble); err != nil {
		return err
	}
	return r.WriteDouble(v)
}

// WriteChar writes "char" value
func (r *request) WriteChar(v rune) error {
	return binary.Write(r.payload, binary.LittleEndian, int16(v))
}

// WriteOChar writes "char" object value
func (r *request) WriteOChar(v Char) error {
	if err := r.WriteByte(typeChar); err != nil {
		return err
	}
	return r.WriteChar(rune(v))
}

// WriteBool writes "bool" value
func (r *request) WriteBool(v bool) error {
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOBool writes "bool" object value
func (r *request) WriteOBool(v bool) error {
	if err := r.WriteByte(typeBool); err != nil {
		return err
	}
	return r.WriteBool(v)
}

// WriteOString writes "string" object value
// String is marshalling as object in all cases.
func (r *request) WriteOString(v string) error {
	if err := r.WriteByte(typeString); err != nil {
		return err
	}
	s := []byte(v)
	if err := r.WriteInt(int32(len(s))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, s)
}

// WriteOTimestamp writes "Timestamp" object value
// Timestamp is marshaled as object in all cases.
func (r *request) WriteOTimestamp(v time.Time) error {
	if err := r.WriteByte(typeTimestamp); err != nil {
		return err
	}
	high := int64(v.Unix() * 1000) // Unix time in milliseconds
	low := v.Nanosecond()
	high += int64(low / int(time.Millisecond))
	low = low % int(time.Millisecond)
	if err := r.WriteLong(high); err != nil {
		return err
	}
	return r.WriteInt(int32(low))
}

// WriteNull writes NULL
func (r *request) WriteNull() error {
	return r.WriteByte(typeNULL)
}

func (r *request) WriteObject(o interface{}) error {
	if o == nil {
		return r.WriteNull()
	}

	switch v := o.(type) {
	case byte:
		return r.WriteOByte(v)
	case int16:
		return r.WriteOShort(v)
	case int32:
		return r.WriteOInt(v)
	case int64:
		return r.WriteOLong(v)
	case float32:
		return r.WriteOFloat(v)
	case float64:
		return r.WriteODouble(v)
	case Char:
		return r.WriteOChar(v)
	case bool:
		return r.WriteOBool(v)
	case string:
		return r.WriteOString(v)
	case time.Time:
		return r.WriteOTimestamp(v)
	default:
		return errors.Errorf("unsupported object type: %s", reflect.TypeOf(v).Name())
	}
}

// WriteTo is function to write request data to io.Writer.
// Returns written bytes.
func (r *request) WriteTo(w io.Writer) (int64, error) {
	return r.payload.WriteTo(w)
}

// newRequest is private constructor for request
func newRequest() request {
	return request{payload: &bytes.Buffer{}}
}
