package ignite

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// Response is interface of base message response functionality
type Response interface {
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

// ReadOString reads "string" object value or NULL (returns "")
func (r *response) ReadOString() (string, error) {
	t, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	switch t {
	case typeNULL:
		return "", nil
	case typeString:
		v, err := r.ReadString()
		return v, err
	default:
		return "", errors.Errorf("invalid type (expected %d, but got %d)", typeString, t)
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

// ReadArrayBytes reads "byte" array value
func (r *response) ReadArrayBytes() ([]byte, error) {
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

// ReadArrayShorts reads "short" array value
func (r *response) ReadArrayShorts() ([]int16, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]int16, l)
	if l > 0 {
		err = binary.Read(r.message, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayInts reads "int" array value
func (r *response) ReadArrayInts() ([]int32, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]int32, l)
	if l > 0 {
		err = binary.Read(r.message, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayLongs reads "long" array value
func (r *response) ReadArrayLongs() ([]int64, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]int64, l)
	if l > 0 {
		err = binary.Read(r.message, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayFloats reads "float" array value
func (r *response) ReadArrayFloats() ([]float32, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]float32, l)
	if l > 0 {
		err = binary.Read(r.message, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayDoubles reads "double" array value
func (r *response) ReadArrayDoubles() ([]float64, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]float64, l)
	if l > 0 {
		err = binary.Read(r.message, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayChars reads "char" array value
func (r *response) ReadArrayChars() ([]Char, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]Char, l)
	for i := 0; i < int(l); i++ {
		if b[i], err = r.ReadChar(); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// ReadArrayBools reads "bool" array value
func (r *response) ReadArrayBools() ([]bool, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]bool, l)
	if l > 0 {
		err = binary.Read(r.message, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayOStrings reads "String" array value
func (r *response) ReadArrayOStrings() ([]string, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]string, l)
	for i := 0; i < int(l); i++ {
		if b[i], err = r.ReadOString(); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// ReadArrayOUUIDs reads "UUID" array value
func (r *response) ReadArrayOUUIDs() ([]uuid.UUID, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]uuid.UUID, l)
	for i := 0; i < int(l); i++ {
		o, err := r.ReadObject()
		if err != nil {
			return nil, err
		}
		b[i] = o.(uuid.UUID)
	}
	return b, nil
}

// ReadArrayODates reads "Date" array value
func (r *response) ReadArrayODates() ([]time.Time, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]time.Time, l)
	for i := 0; i < int(l); i++ {
		o, err := r.ReadObject()
		if err != nil {
			return nil, err
		}
		b[i] = o.(time.Time)
	}
	return b, nil
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

// ReadArrayOTimestamps reads "Timestamp" array value
func (r *response) ReadArrayOTimestamps() ([]time.Time, error) {
	l, err := r.ReadInt()
	if err != nil {
		return nil, err
	}
	b := make([]time.Time, l)
	for i := 0; i < int(l); i++ {
		o, err := r.ReadObject()
		if err != nil {
			return nil, err
		}
		b[i] = o.(time.Time)
	}
	return b, nil
}

// ReadTime reads "Time" object value
func (r *response) ReadTime() (time.Time, error) {
	v, err := r.ReadLong()
	if err != nil {
		return time.Time{}, err
	}
	t := time.Unix(int64(v)/1000, (int64(v)%1000)*int64(time.Millisecond)).UTC()
	return time.Date(1, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC), nil
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
		return r.ReadArrayBytes()
	case typeShortArray:
		return r.ReadArrayShorts()
	case typeIntArray:
		return r.ReadArrayInts()
	case typeLongArray:
		return r.ReadArrayLongs()
	case typeFloatArray:
		return r.ReadArrayFloats()
	case typeDoubleArray:
		return r.ReadArrayDoubles()
	case typeCharArray:
		return r.ReadArrayChars()
	case typeBoolArray:
		return r.ReadArrayBools()
	case typeStringArray:
		return r.ReadArrayOStrings()
	case typeDateArray:
		return r.ReadArrayODates()
	case typeUUIDArray:
		return r.ReadArrayOUUIDs()
	case typeTimestamp:
		return r.ReadTimestamp()
	case typeTimestampArray:
		return r.ReadArrayOTimestamps()
	case typeTime:
		return r.ReadTime()
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
