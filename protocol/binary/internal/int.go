package internal

import (
	"encoding/binary"
	"io"
	"strconv"

	"github.com/amsokol/go-errors"
)

const (
	typeInt = 3
)

// Int is Apache Ignite int type
type Int struct {
	v    int32
	meta bool
	Type
}

// Write writes Int object to the stream
func (i *Int) Write(w io.Writer) error {
	if i.meta {
		// Type code
		if err := binary.Write(w, binary.LittleEndian, int8(typeInt)); err != nil {
			return errors.Wrapf(err, "failed to Int type code")
		}
	}

	// Int value
	if err := binary.Write(w, binary.LittleEndian, i.v); err != nil {
		return errors.Wrapf(err, "failed to Int value")
	}

	return nil
}

// Size returns Int object size in bytes
func (i *Int) Size() int {
	size := 4
	if i.meta {
		size++
	}
	return size
}

// Read reads Int object
func (i *Int) Read(r io.Reader) error {
	if i.meta {
		code, err := ReadTypeCode(r)
		if err != nil {
			return errors.Wrapf(err, "failed to read type code")
		}
		if code != typeInt {
			return errors.Newff(errors.Fields{"code": code}, "invalid type code for 'int', expecting "+
				strconv.Itoa(typeInt))
		}
	}
	if err := binary.Read(r, binary.LittleEndian, &i.v); err != nil {
		return errors.Wrapf(err, "failed to read Int value")
	}
	return nil
}

// NewInt creates Int object
func NewInt(i int, meta bool) *Int {
	return &Int{v: int32(i), meta: meta}
}
