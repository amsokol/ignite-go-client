package ignite

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/google/uuid"
)

// Date is Unix time, the number of MILLISECONDS elapsed
// since January 1, 1970 UTC.
type Date int64

const (
	typeString = 9
	typeNULL   = 101
)

// const vars
var (
	objectTypes = map[string]byte{
		reflect.TypeOf(byte(0)).Name():       1,
		reflect.TypeOf(int16(0)).Name():      2,
		reflect.TypeOf(int32(0)).Name():      3,
		reflect.TypeOf(int64(0)).Name():      4,
		reflect.TypeOf(float32(0)).Name():    5,
		reflect.TypeOf(float64(0)).Name():    6,
		reflect.TypeOf('a').Name():           7,
		reflect.TypeOf(true).Name():          8,
		reflect.TypeOf("").Name():            9,
		reflect.TypeOf(uuid.UUID{}).Name():   10,
		reflect.TypeOf(Date(0)).Name():       11,
		reflect.TypeOf([]byte{}).Name():      12,
		reflect.TypeOf([]int16{}).Name():     13,
		reflect.TypeOf([]int32{}).Name():     14,
		reflect.TypeOf([]int64{}).Name():     15,
		reflect.TypeOf([]float32{}).Name():   16,
		reflect.TypeOf([]float64{}).Name():   17,
		reflect.TypeOf([]rune{}).Name():      18,
		reflect.TypeOf([]bool{}).Name():      19,
		reflect.TypeOf([]string{}).Name():    20,
		reflect.TypeOf([]uuid.UUID{}).Name(): 21,
		reflect.TypeOf([]Date{}).Name():      22,
		// TODO: Object array = 23
		// TODO: Collection = 24
		// TODO: Map = 25
		// TODO: Enum = 28
		// TODO: Enum Array = 29
		// TODO: Decimal = 30
		// TODO: Decimal Array = 31
		// TODO: Timestamp = 33
		// TODO: Timestamp Array = 34
		// TODO: Time = 36
		// TODO: Time Array = 37
	}
)

func write(w io.Writer, data ...interface{}) error {
	var err error
	for i, d := range data {
		switch v := d.(type) {
		case string:
			err = writeString(w, v)
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

func writeString(w io.Writer, data string) error {
	var err error
	// Type code
	if err = binary.Write(w, binary.LittleEndian, byte(typeString)); err == nil {
		s := []byte(data)
		// String data length
		length := int32(len(s))
		if err = binary.Write(w, binary.LittleEndian, length); err == nil {
			if length > 0 {
				// String data
				err = binary.Write(w, binary.LittleEndian, s)
			}
		}
	}
	return err
}

func read(r io.Reader, data ...interface{}) error {
	var err error
	for i, d := range data {
		switch v := d.(type) {
		case *string:
			err = readString(r, v)
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

func readString(r io.Reader, data *string) error {
	var err error
	// Type code
	var code int8
	if err = binary.Read(r, binary.LittleEndian, &code); err == nil {
		if code != typeNULL {
			if code == typeString {
				// String data length
				var length int32
				if err = binary.Read(r, binary.LittleEndian, &length); err == nil {
					if length > 0 {
						s := make([]byte, length)
						// String data
						if err = binary.Read(r, binary.LittleEndian, &s); err == nil {
							*data = string(s)
						}
					} else {
						*data = ""
					}
				}
			} else {
				err = fmt.Errorf("invalid type code for 'String' expecting %d, but received %d", typeString, code)
			}
		} else {
			*data = ""
		}
	}
	return err
}
