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

// Char is UTF32 symbol type
type Char rune

// NULL is synthetic type for NULL value
type NULL int

const (
	// Supported standard types and their type codes are as follows:
	typeByte        = 1
	typeShort       = 2
	typeInt         = 3
	typeLong        = 4
	typeFloat       = 5
	typeDouble      = 6
	typeChar        = 7
	typeBool        = 8
	typeString      = 9
	typeUUID        = 10
	typeDate        = 11
	typeByteArray   = 12
	typeShortArray  = 13
	typeIntArray    = 14
	typeLongArray   = 15
	typeFloatArray  = 16
	typeDoubleArray = 17
	typeCharArray   = 18
	typeBoolArray   = 19
	typeStringArray = 20
	typeUUIDArray   = 21
	typeDateArray   = 22

	typeNULL = 101

	// Null is nil value for object
	Null = NULL(0)
)

// const vars
var (
	objectTypes = map[string]byte{
		reflect.TypeOf(byte(0)).Name():       typeByte,
		reflect.TypeOf(int16(0)).Name():      typeShort,
		reflect.TypeOf(int32(0)).Name():      typeInt,
		reflect.TypeOf(int64(0)).Name():      typeLong,
		reflect.TypeOf(float32(0)).Name():    typeFloat,
		reflect.TypeOf(float64(0)).Name():    typeDouble,
		reflect.TypeOf(Char('a')).Name():     typeChar,
		reflect.TypeOf(true).Name():          typeBool,
		reflect.TypeOf("").Name():            typeString,
		reflect.TypeOf(uuid.UUID{}).Name():   typeUUID,
		reflect.TypeOf(Date(0)).Name():       typeDate,
		reflect.TypeOf([]byte{}).Name():      typeByteArray,
		reflect.TypeOf([]int16{}).Name():     typeShortArray,
		reflect.TypeOf([]int32{}).Name():     typeIntArray,
		reflect.TypeOf([]int64{}).Name():     typeLongArray,
		reflect.TypeOf([]float32{}).Name():   typeFloatArray,
		reflect.TypeOf([]float64{}).Name():   typeDoubleArray,
		reflect.TypeOf([]rune{}).Name():      typeCharArray,
		reflect.TypeOf([]bool{}).Name():      typeBoolArray,
		reflect.TypeOf([]string{}).Name():    typeStringArray,
		reflect.TypeOf([]uuid.UUID{}).Name(): typeUUIDArray,
		reflect.TypeOf([]Date{}).Name():      typeDateArray,
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
		// reflect.TypeOf(Null).Name(): typeNULL,
	}
)

func writePrimitives(w io.Writer, primitives ...interface{}) error {
	var err error
	for i, d := range primitives {
		switch v := d.(type) {
		case string:
			err = writeObject(w, v)
		default:
			err = binary.Write(w, binary.LittleEndian, v)
		}
		if err != nil {
			err = fmt.Errorf("failed to write primitive with index %d, reason: %s", i, err.Error())
			break
		}
	}
	return err
}

func writeObjects(w io.Writer, objects ...interface{}) error {
	var err error
	for i, d := range objects {
		err = writeObject(w, d)
		if err != nil {
			err = fmt.Errorf("failed to write object with index %d, reason: %s", i, err.Error())
			break
		}
	}
	return err
}

func writeObject(w io.Writer, object interface{}) error {
	if object == nil {
		return binary.Write(w, binary.LittleEndian, byte(typeNULL))
	}
	var err error
	switch v := object.(type) {
	case byte, int16, int32, int64, float32, float64, bool:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case Char:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			err = binary.Write(w, binary.LittleEndian, int16(v))
		}
	case string:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			s := []byte(v)
			// String data length
			length := int32(len(s))
			if err = binary.Write(w, binary.LittleEndian, length); err == nil {
				if length > 0 {
					// String data
					err = binary.Write(w, binary.LittleEndian, s)
				}
			}
		}
	case uuid.UUID:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if d, err := v.MarshalBinary(); err == nil {
				err = binary.Write(w, binary.LittleEndian, d)
			}
		}
	case Date:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			err = binary.Write(w, binary.LittleEndian, int64(v))
		}
	case []byte:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []int16:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []int32:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []int64:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []float32:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []float64:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []bool:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []Char:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				for _, c := range v {
					if err = binary.Write(w, binary.LittleEndian, int16(c)); err != nil {
						break
					}
				}
			}
		}
	case []string:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				for _, s := range v {
					if err = writeString(w, s); err != nil {
						break
					}
				}
			}
		}
	case []uuid.UUID:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				for _, u := range v {
					if d, err := u.MarshalBinary(); err == nil {
						err = binary.Write(w, binary.LittleEndian, d)
					}
				}
			}
		}
	case []Date:
		if err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()]); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				for _, d := range v {
					if err = binary.Write(w, binary.LittleEndian, int64(d)); err != nil {
						break
					}
				}
			}
		}
	/*
		case NULL:
			err = binary.Write(w, binary.LittleEndian, objectTypes[reflect.TypeOf(v).Name()])
	*/
	default:
		err = fmt.Errorf("unsupported object type: %s", reflect.TypeOf(v).Name())
	}
	return err
}

func writeString(w io.Writer, str string) error {
	var err error
	// Type code
	if err = binary.Write(w, binary.LittleEndian, byte(typeString)); err == nil {
		s := []byte(str)
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
