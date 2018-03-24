package ignite

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	typeString = 9
)

func write(w io.Writer, data ...interface{}) error {
	var err error
	for i, d := range data {
		switch v := d.(type) {
		case string:
			// Type code
			if err = binary.Write(w, binary.LittleEndian, byte(typeString)); err == nil {
				s := []byte(v)
				// String data length
				if err = binary.Write(w, binary.LittleEndian, int32(len(s))); err == nil {
					// String data
					err = binary.Write(w, binary.LittleEndian, s)
				}
			}
		default:
			err = binary.Write(w, binary.LittleEndian, v)
		}
		if err != nil {
			err = fmt.Errorf("failed to write data with index %d, reason: %s", i, err.Error())
			break
		}
	}
	return err
}

func read(r io.Reader, data ...interface{}) error {
	var err error
	for i, d := range data {
		switch v := d.(type) {
		case *string:
			// Type code
			var code int8
			if err = binary.Read(r, binary.LittleEndian, &code); err == nil {
				if code != typeString {
					return fmt.Errorf("invalid type code for 'String', expecting %d, but received %d",
						typeString, code)
				}
				// String data length
				var length int32
				if err = binary.Read(r, binary.LittleEndian, &length); err == nil {
					s := make([]byte, length)
					// String data
					if err = binary.Read(r, binary.LittleEndian, &s); err == nil {
						*v = string(s)
					}
				}
			}
		default:
			err = binary.Read(r, binary.LittleEndian, v)
		}
		if err != nil {
			err = fmt.Errorf("failed to read data with index %d, reason: %s", i, err.Error())
			break
		}
	}
	return err
}
