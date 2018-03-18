package internal

import (
	"encoding/binary"
	"io"
	"strconv"

	"github.com/amsokol/go-errors"
)

const (
	typeString     = 9
	typeStringSize = 1 + 4
)

// String is Apache Ignite string type
type String struct {
	v []byte
	Type
}

// Write writes Sting object to the stream
func (s *String) Write(w io.Writer) error {
	// Type code
	if err := binary.Write(w, binary.LittleEndian, int8(typeString)); err != nil {
		return errors.Wrapf(err, "failed to String type code")
	}

	// String data length
	if err := binary.Write(w, binary.LittleEndian, int32(len(s.v))); err != nil {
		return errors.Wrapf(err, "failed to String data length")
	}

	// String data
	if err := binary.Write(w, binary.LittleEndian, s.v); err != nil {
		return errors.Wrapf(err, "failed to String data")
	}

	return nil
}

// Size returns String object size in bytes
func (s *String) Size() int {
	return typeStringSize + len(s.v)
}

// Read reads String object
func (s *String) Read(r io.Reader) error {
	code, err := ReadTypeCode(r)
	if err != nil {
		return errors.Wrapf(err, "failed to read type code")
	}
	if code != typeString {
		return errors.Newff(errors.Fields{"code": code}, "invalid type code for 'String', expecting "+
			strconv.Itoa(typeString))
	}
	var len int32
	if err := binary.Read(r, binary.LittleEndian, &len); err != nil {
		return errors.Wrapf(err, "failed to read String length")
	}
	s.v = make([]byte, len)
	if err := binary.Read(r, binary.LittleEndian, &s.v); err != nil {
		return errors.Wrapf(err, "failed to read String data")
	}
	return nil
}

// Value return golang value of String object
func (s *String) Value() string {
	return string(s.v)
}

// NewString creates Sting object
func NewString(s string) *String {
	return &String{v: []byte(s)}
}
