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

// WriteOArrayByte writes "byte" array object value
func (r *request) WriteOArrayByte(v []byte) error {
	if err := r.WriteByte(typeByteArray); err != nil {
		return err
	}
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOArrayShort writes "short" array object value
func (r *request) WriteOArrayShort(v []int16) error {
	if err := r.WriteByte(typeShortArray); err != nil {
		return err
	}
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOArrayInt writes "int" array object value
func (r *request) WriteOArrayInt(v []int32) error {
	if err := r.WriteByte(typeIntArray); err != nil {
		return err
	}
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOArrayLong writes "long" array object value
func (r *request) WriteOArrayLong(v []int64) error {
	if err := r.WriteByte(typeLongArray); err != nil {
		return err
	}
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOArrayFloat writes "float" array object value
func (r *request) WriteOArrayFloat(v []float32) error {
	if err := r.WriteByte(typeFloatArray); err != nil {
		return err
	}
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOArrayDouble writes "double" array object value
func (r *request) WriteOArrayDouble(v []float64) error {
	if err := r.WriteByte(typeDoubleArray); err != nil {
		return err
	}
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOArrayChar writes "char" array object value
func (r *request) WriteOArrayChar(v []Char) error {
	if err := r.WriteByte(typeCharArray); err != nil {
		return err
	}
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

// WriteOArrayBool writes "Bool" array object value
func (r *request) WriteOArrayBool(v []bool) error {
	if err := r.WriteByte(typeBoolArray); err != nil {
		return err
	}
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	return binary.Write(r.payload, binary.LittleEndian, v)
}

// WriteOArrayOString writes "String" array object value
func (r *request) WriteOArrayOString(v []string) error {
	if err := r.WriteByte(typeStringArray); err != nil {
		return err
	}
	if err := r.WriteInt(int32(len(v))); err != nil {
		return err
	}
	for _, s := range v {
		if err := r.WriteOString(s); err != nil {
			return err
		}
	}
	return nil
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
		return r.WriteOArrayByte(v)
	case []int16:
		return r.WriteOArrayShort(v)
	case []int32:
		return r.WriteOArrayInt(v)
	case []int64:
		return r.WriteOArrayLong(v)
	case []float32:
		return r.WriteOArrayFloat(v)
	case []float64:
		return r.WriteOArrayDouble(v)
	case []Char:
		return r.WriteOArrayChar(v)
	case []bool:
		return r.WriteOArrayBool(v)
	case []string:
		return r.WriteOArrayOString(v)
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
