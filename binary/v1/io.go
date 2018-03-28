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

const (
	// Supported standard types and their type codes are as follows:
	typeByte   byte = 1
	typeShort  byte = 2
	typeInt    byte = 3
	typeLong   byte = 4
	typeFloat  byte = 5
	typeDouble byte = 6
	typeChar   byte = 7
	typeBool   byte = 8
	typeString byte = 9
	typeUUID   byte = 10
	/*
	   Apache.Ignite.Core.Impl.Binary.BinaryUtils

	   bytes[0] = jBytes[4]; // a1
	   bytes[1] = jBytes[5]; // a2
	   bytes[2] = jBytes[6]; // a3
	   bytes[3] = jBytes[7]; // a4

	   bytes[4] = jBytes[2]; // b1
	   bytes[5] = jBytes[3]; // b2

	   bytes[6] = jBytes[0]; // c1
	   bytes[7] = jBytes[1]; // c2

	   bytes[8] = jBytes[15]; // d
	   bytes[9] = jBytes[14]; // e
	   bytes[10] = jBytes[13]; // f
	   bytes[11] = jBytes[12]; // g
	   bytes[12] = jBytes[11]; // h
	   bytes[13] = jBytes[10]; // i
	   bytes[14] = jBytes[9]; // j
	   bytes[15] = jBytes[8]; // k
	*/
	typeDate        byte = 11
	typeByteArray   byte = 12
	typeShortArray  byte = 13
	typeIntArray    byte = 14
	typeLongArray   byte = 15
	typeFloatArray  byte = 16
	typeDoubleArray byte = 17
	typeCharArray   byte = 18
	typeBoolArray   byte = 19
	typeStringArray byte = 20
	typeUUIDArray   byte = 21
	typeDateArray   byte = 22
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
	typeNULL byte = 101
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
		return binary.Write(w, binary.LittleEndian, typeNULL)
	}
	var err error
	switch v := object.(type) {
	case byte:
		if err = binary.Write(w, binary.LittleEndian, typeByte); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case int16:
		if err = binary.Write(w, binary.LittleEndian, typeShort); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case int32:
		if err = binary.Write(w, binary.LittleEndian, typeInt); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case int64:
		if err = binary.Write(w, binary.LittleEndian, typeLong); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case float32:
		if err = binary.Write(w, binary.LittleEndian, typeFloat); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case float64:
		if err = binary.Write(w, binary.LittleEndian, typeDouble); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case bool:
		if err = binary.Write(w, binary.LittleEndian, typeBool); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case Char:
		if err = binary.Write(w, binary.LittleEndian, typeChar); err == nil {
			err = binary.Write(w, binary.LittleEndian, int16(v))
		}
	case string:
		if err = binary.Write(w, binary.LittleEndian, typeString); err == nil {
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
		if err = binary.Write(w, binary.LittleEndian, typeUUID); err == nil {
			err = binary.Write(w, binary.LittleEndian, v)
		}
	case Date:
		if err = binary.Write(w, binary.LittleEndian, typeDate); err == nil {
			err = binary.Write(w, binary.LittleEndian, int64(v))
		}
	case []byte:
		if err = binary.Write(w, binary.LittleEndian, typeByteArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []int16:
		if err = binary.Write(w, binary.LittleEndian, typeShortArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []int32:
		if err = binary.Write(w, binary.LittleEndian, typeIntArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []int64:
		if err = binary.Write(w, binary.LittleEndian, typeLongArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []float32:
		if err = binary.Write(w, binary.LittleEndian, typeFloatArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []float64:
		if err = binary.Write(w, binary.LittleEndian, typeDoubleArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []bool:
		if err = binary.Write(w, binary.LittleEndian, typeBoolArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				err = binary.Write(w, binary.LittleEndian, v)
			}
		}
	case []Char:
		if err = binary.Write(w, binary.LittleEndian, typeCharArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				for _, c := range v {
					if err = binary.Write(w, binary.LittleEndian, int16(c)); err != nil {
						break
					}
				}
			}
		}
	case []string:
		if err = binary.Write(w, binary.LittleEndian, typeStringArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				for _, s := range v {
					if err = writeString(w, s); err != nil {
						break
					}
				}
			}
		}
	/* TODO: contact Apache Ignite team for support
	case []uuid.UUID:
		if err = binary.Write(w, binary.LittleEndian, typeUUIDArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				for _, d := range v {
					if err = binary.Write(w, binary.LittleEndian, typeUUID); err == nil {
						break
					}
					if err = binary.Write(w, binary.LittleEndian, d); err != nil {
						break
					}
				}
			}
		}
	*/
	/* TODO: contact Apache Ignite team for support
	case []Date:
		if err = binary.Write(w, binary.LittleEndian, typeDateArray); err == nil {
			if err = binary.Write(w, binary.LittleEndian, int32(len(v))); err == nil {
				for _, d := range v {
					if err = binary.Write(w, binary.LittleEndian, int64(d)); err != nil {
						break
					}
				}
			}
		}
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

func readPrimitives(r io.Reader, data ...interface{}) error {
	var err error
	for i, d := range data {
		switch v := d.(type) {
		case *string:
			err = readString(r, true, v)
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

func readString(r io.Reader, withCode bool, data *string) error {
	if withCode {
		// Type code
		var code byte
		if err := binary.Read(r, binary.LittleEndian, &code); err != nil {
			return fmt.Errorf("failed to read string code: %s", err.Error())
		}
		if code == typeNULL {
			*data = ""
			return nil
		}
		if code != typeString {
			return fmt.Errorf("invalid type code for 'String' expecting %d, but received %d", typeString, code)
		}
	}
	var err error
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
	return err
}

func readObjects(r io.Reader, count int) ([]interface{}, error) {
	objects := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		o, err := readObject(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read object with index %d, reason: %s", i, err.Error())
		}
		objects = append(objects, o)
	}
	return objects, nil
}

func readObject(r io.Reader) (interface{}, error) {
	var code byte
	if err := readPrimitives(r, &code); err != nil {
		return nil, fmt.Errorf("failed to read object type code: %s", err.Error())
	}

	switch code {
	case typeByte:
		var o byte
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read byte object value: %s", err.Error())
		}
		return o, nil
	case typeShort:
		var o int16
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read short object value: %s", err.Error())
		}
		return o, nil
	case typeInt:
		var o int32
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read int object value: %s", err.Error())
		}
		return o, nil
	case typeLong:
		var o int64
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read long object value: %s", err.Error())
		}
		return o, nil
	case typeFloat:
		var o float32
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read float object value: %s", err.Error())
		}
		return o, nil
	case typeDouble:
		var o float64
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read double object value: %s", err.Error())
		}
		return o, nil
	case typeChar:
		var o int16
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read char object value: %s", err.Error())
		}
		return Char(o), nil
	case typeBool:
		var o bool
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read bool object value: %s", err.Error())
		}
		return o, nil
	case typeString:
		var o string
		if err := readString(r, false, &o); err != nil {
			return nil, fmt.Errorf("failed to read string object value: %s", err.Error())
		}
		return o, nil
	case typeUUID:
		var o uuid.UUID
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read UUID object value: %s", err.Error())
		}
		return o, nil
	case typeDate:
		var o int64
		if err := readPrimitives(r, &o); err != nil {
			return nil, fmt.Errorf("failed to read Date object value: %s", err.Error())
		}
		return Date(o), nil
	case typeByteArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read byte array object length: %s", err.Error())
		}
		o := make([]byte, length, length)
		if length > 0 {
			if err := readPrimitives(r, &o); err != nil {
				return nil, fmt.Errorf("failed to read byte array object value: %s", err.Error())
			}
		}
		return o, nil
	case typeShortArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read short array object length: %s", err.Error())
		}
		o := make([]int16, length, length)
		if length > 0 {
			if err := readPrimitives(r, &o); err != nil {
				return nil, fmt.Errorf("failed to read short array object value: %s", err.Error())
			}
		}
		return o, nil
	case typeIntArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read int array object length: %s", err.Error())
		}
		o := make([]int32, length, length)
		if length > 0 {
			if err := readPrimitives(r, &o); err != nil {
				return nil, fmt.Errorf("failed to read int array object value: %s", err.Error())
			}
		}
		return o, nil
	case typeLongArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read long array object length: %s", err.Error())
		}
		o := make([]int64, length, length)
		if length > 0 {
			if err := readPrimitives(r, &o); err != nil {
				return nil, fmt.Errorf("failed to read long array object value: %s", err.Error())
			}
		}
		return o, nil
	case typeFloatArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read float array object length: %s", err.Error())
		}
		o := make([]float32, length, length)
		if length > 0 {
			if err := readPrimitives(r, &o); err != nil {
				return nil, fmt.Errorf("failed to read float array object value: %s", err.Error())
			}
		}
		return o, nil
	case typeDoubleArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read double array object length: %s", err.Error())
		}
		o := make([]float64, length, length)
		if length > 0 {
			if err := readPrimitives(r, &o); err != nil {
				return nil, fmt.Errorf("failed to read double array object value: %s", err.Error())
			}
		}
		return o, nil
	case typeCharArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read char array object length: %s", err.Error())
		}
		o := make([]Char, 0, length)
		for i := 0; i < int(length); i++ {
			var v int16
			if err := readPrimitives(r, &v); err != nil {
				return nil, fmt.Errorf("failed to read char array object value: %s", err.Error())
			}
			o = append(o, Char(v))
		}
		return o, nil
	case typeBoolArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read bool array object length: %s", err.Error())
		}
		o := make([]bool, length, length)
		if length > 0 {
			if err := readPrimitives(r, &o); err != nil {
				return nil, fmt.Errorf("failed to read bool array object value: %s", err.Error())
			}
		}
		return o, nil
	case typeStringArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read string array object length: %s", err.Error())
		}
		o := make([]string, 0, length)
		for i := 0; i < int(length); i++ {
			var s string
			if err := readPrimitives(r, &s); err != nil {
				return nil, fmt.Errorf("failed to read string array object value: %s", err.Error())
			}
			o = append(o, s)
		}
		return o, nil
	case typeUUIDArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read UUID array object length: %s", err.Error())
		}
		o := make([]uuid.UUID, 0, length)
		for i := 0; i < int(length); i++ {
			var v uuid.UUID
			if err := readPrimitives(r, &v); err != nil {
				return nil, fmt.Errorf("failed to read UUID array object value: %s", err.Error())
			}
			o = append(o, v)
		}
		return o, nil
	case typeDateArray:
		var length int32
		if err := readPrimitives(r, &length); err != nil {
			return nil, fmt.Errorf("failed to read Date array object length: %s", err.Error())
		}
		o := make([]Date, 0, length)
		for i := 0; i < int(length); i++ {
			var v int64
			if err := readPrimitives(r, &v); err != nil {
				return nil, fmt.Errorf("failed to read UUID array object value: %s", err.Error())
			}
			o = append(o, Date(v))
		}
		return o, nil
	case typeNULL:
		return nil, nil
	default:
		return nil, fmt.Errorf("type with code %d is not supported", code)
	}
}
