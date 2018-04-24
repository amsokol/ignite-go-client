package ignite

import (
	"bytes"
	"encoding/binary"
	"io"
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

	// WriteOString writes "string" object value
	// String is marshaled as object in all cases.
	WriteOString(v string) error

	// WriteTo is abstract function to write request data to io.Writer.
	// Each child struct have to implement this function.
	// Returns written bytes.
	WriteTo(w io.Writer) (int64, error)
}

// request is abstract struct is implementing base message request functionality
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
