package ignite

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"time"

	"github.com/google/uuid"

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
	WriteChar(v Char) error
	// WriteOChar writes "char" object value
	WriteOChar(v Char) error

	// WriteBool writes "bool" value
	WriteBool(v bool) error
	// WriteOBool writes "bool" object value
	WriteOBool(v bool) error

	// WriteOString writes "string" object value
	// String is marshaled as object in all cases.
	WriteOString(v string) error

	// WriteOUUID writes "UUID" object value
	// UUID is marshaled as object in all cases.
	WriteOUUID(v uuid.UUID) error

	// WriteODate writes "Date" object value
	WriteODate(v Date) error

	// WriteByteArray writes "byte" array value
	WriteByteArray(v []byte) error
	// WriteOByteArray writes "byte" array object value
	WriteOByteArray(v []byte) error

	// WriteShortArray writes "short" array value
	WriteShortArray(v []int16) error
	// WriteOShortArray writes "short" array object value
	WriteOShortArray(v []int16) error

	// WriteIntArray writes "int" array value
	WriteIntArray(v []int32) error
	// WriteOIntArray writes "int" array object value
	WriteOIntArray(v []int32) error

	// WriteLongArray writes "long" array value
	WriteLongArray(v []int64) error
	// WriteOLongArray writes "long" array object value
	WriteOLongArray(v []int64) error

	// WriteFloatArray writes "float" array value
	WriteFloatArray(v []float32) error
	// WriteOFloatArray writes "float" array object value
	WriteOFloatArray(v []float32) error

	// WriteDoubleArray writes "double" array value
	WriteDoubleArray(v []float64) error
	// WriteODoubleArray writes "double" array object value
	WriteODoubleArray(v []float64) error

	// WriteCharArray writes "char" array value
	WriteCharArray(v []Char) error
	// WriteOCharArray writes "char" array object value
	WriteOCharArray(v []Char) error

	// WriteBoolArray writes "bool" array value
	WriteBoolArray(v []bool) error
	// WriteOBoolArray writes "bool" array object value
	WriteOBoolArray(v []bool) error

	// WriteOTimestamp writes "Timestamp" object value
	// Timestamp is marshaled as object in all cases.
	WriteOTimestamp(v time.Time) error

	// WriteOTime writes "Time" object value
	// Time is marshaled as object in all cases.
	WriteOTime(v Time) error

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
func (r *request) WriteChar(v Char) error {
	return binary.Write(r.payload, binary.LittleEndian, int16(v))
}

// WriteOChar writes "char" object value
func (r *request) WriteOChar(v Char) error {
	if err := r.WriteByte(typeChar); err != nil {
		return err
	}
	return r.WriteChar(v)
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

// WriteOUUID writes "UUID" object value
// UUID is marshaled as object in all cases.
func (r *request) WriteOUUID(v uuid.UUID) error {
	if err := r.WriteByte(typeUUID); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteODate writes "Date" object value
func (r *request) WriteODate(v Date) error {
	if err := r.WriteByte(typeDate); err != nil {
		return err
	}
	return r.WriteLong(int64(v))
}

// WriteByteArray writes "byte" array value
func (r *request) WriteByteArray(v []byte) error {
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOByteArray writes "byte" array object value
func (r *request) WriteOByteArray(v []byte) error {
	if err := r.WriteByte(typeByteArray); err != nil {
		return err
	}
	return r.WriteByteArray(v)
}

// WriteShortArray writes "short" array value
func (r *request) WriteShortArray(v []int16) error {
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOShortArray writes "short" array object value
func (r *request) WriteOShortArray(v []int16) error {
	if err := r.WriteByte(typeShortArray); err != nil {
		return err
	}
	return r.WriteShortArray(v)
}

// WriteIntArray writes "int" array value
func (r *request) WriteIntArray(v []int32) error {
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOIntArray writes "int" array object value
func (r *request) WriteOIntArray(v []int32) error {
	if err := r.WriteByte(typeIntArray); err != nil {
		return err
	}
	return r.WriteIntArray(v)
}

// WriteLongArray writes "long" array value
func (r *request) WriteLongArray(v []int64) error {
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOLongArray writes "long" array object value
func (r *request) WriteOLongArray(v []int64) error {
	if err := r.WriteByte(typeLongArray); err != nil {
		return err
	}
	return r.WriteLongArray(v)
}

// WriteFloatArray writes "float" array value
func (r *request) WriteFloatArray(v []float32) error {
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOFloatArray writes "float" array object value
func (r *request) WriteOFloatArray(v []float32) error {
	if err := r.WriteByte(typeFloatArray); err != nil {
		return err
	}
	return r.WriteFloatArray(v)
}

// WriteDoubleArray writes "double" array value
func (r *request) WriteDoubleArray(v []float64) error {
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteODoubleArray writes "double" array object value
func (r *request) WriteODoubleArray(v []float64) error {
	if err := r.WriteByte(typeDoubleArray); err != nil {
		return err
	}
	return r.WriteDoubleArray(v)
}

// WriteCharArray writes "char" array value
func (r *request) WriteCharArray(v []Char) error {
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	for _, c := range v {
		if err := r.WriteChar(c); err != nil {
			return err
		}
	}
	return nil
}

// WriteOCharArray writes "char" array object value
func (r *request) WriteOCharArray(v []Char) error {
	if err := r.WriteByte(typeCharArray); err != nil {
		return err
	}
	return r.WriteCharArray(v)
}

// WriteBoolArray writes "bool" array value
func (r *request) WriteBoolArray(v []bool) error {
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOBoolArray writes "Bool" array object value
func (r *request) WriteOBoolArray(v []bool) error {
	if err := r.WriteByte(typeBoolArray); err != nil {
		return err
	}
	return r.WriteBoolArray(v)
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

// WriteOTime writes "Time" object value
// Time is marshaled as object in all cases.
func (r *request) WriteOTime(v Time) error {
	if err := r.WriteByte(typeTime); err != nil {
		return err
	}
	return r.WriteLong(int64(v))
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
	case uuid.UUID:
		return r.WriteOUUID(v)
	case Date:
		return r.WriteODate(v)
	case []byte:
		return r.WriteOByteArray(v)
	case []int16:
		return r.WriteOShortArray(v)
	case []int32:
		return r.WriteOIntArray(v)
	case []int64:
		return r.WriteOLongArray(v)
	case []float32:
		return r.WriteOFloatArray(v)
	case []float64:
		return r.WriteODoubleArray(v)
	case []Char:
		return r.WriteOCharArray(v)
	case []bool:
		return r.WriteOBoolArray(v)
	case time.Time:
		return r.WriteOTimestamp(v)
	case Time:
		return r.WriteOTime(v)
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
