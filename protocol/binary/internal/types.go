package internal

import (
	"io"
)

const (
	typeByte   = 1
	typeShort  = 2
	typeLong   = 4
	typeFloat  = 5
	typeDouble = 6
	typeChar   = 7
	typeBool   = 8
)

// Type is common interface for each Apache Ignite type
type Type interface {
	Size() int
	Read(r io.Reader) error
	Write(w io.Writer) error
}

/*
// ReadByte reads byte
func ReadByte(r io.Reader) (byte, error) {
	var code byte
	if err := binary.Read(r, binary.LittleEndian, &code); err != nil {
		return 0, errors.Wrapf(err, "failed to read type code")
	}
	if code != typeByte {
		return 0, errors.Newff(errors.Fields{"code": code}, "invalid type code for byte, expecting "+
			strconv.Itoa(typeByte))
	}
	var b byte
	if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
		return 0, errors.Wrapf(err, "failed to read byte")
	}
	return b, nil
}
*/
