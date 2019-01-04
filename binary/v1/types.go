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
	// TODO: Object array = 23
	// TODO: Collection = 24
	// TODO: Map = 25
	typeBinaryObjectArray = 27
	// TODO: Enum = 28
	// TODO: Enum Array = 29
	// TODO: Decimal = 30
	// TODO: Decimal Array = 31
	typeTimestamp      = 33
	typeTimestampArray = 34
	typeTime           = 36
	typeTimeArray      = 37
	typeNULL           = 101
	typeComplexObject  = 103
)

const (
	// ComplexObjectHeaderLength is complex object header length
	ComplexObjectHeaderLength = 24

	// ComplexObjectVersion is version of complex format
	ComplexObjectVersion = 1

	// ComplexObjectUserType is user type FLAG_USR_TYP = 0x0001
	ComplexObjectUserType = 0x0001
	// ComplexObjectHasSchema is only raw data exists FLAG_HAS_SCHEMA = 0x0002
	ComplexObjectHasSchema = 0x0002
	// ComplexObjectHasRaw is indicating that object has raw data FLAG_HAS_RAW = 0x0004
	ComplexObjectHasRaw = 0x0004
	// ComplexObjectOffsetOneByte is offsets take 1 byte FLAG_OFFSET_ONE_BYTE = 0x0008
	ComplexObjectOffsetOneByte = 0x0008
	// ComplexObjectOffsetTwoBytes is offsets take 2 bytes FLAG_OFFSET_TWO_BYTES = 0x0010
	ComplexObjectOffsetTwoBytes = 0x0010
	// ComplexObjectCompactFooter is compact footer, no field IDs. FLAG_COMPACT_FOOTER = 0x0020
	ComplexObjectCompactFooter = 0x0020
)

// Char is Apache Ignite "char" type
type Char rune

// Date is Unix time, the number of MILLISECONDS elapsed
// since January 1, 1970 UTC.
type Date int64

// ComplexObject is "complex object" type
type ComplexObject struct {
	Type   int32
	Fields map[int32]interface{}
}

// Set sets field value
func (c *ComplexObject) Set(field string, value interface{}) {
	c.Fields[HashCode(field)] = value
}

// Get gets field value
func (c *ComplexObject) Get(field string) (interface{}, bool) {
	v, ok := c.Fields[HashCode(field)]
	return v, ok
}

// NewComplexObject is constructor for ComplexObject
func NewComplexObject(typeName string) ComplexObject {
	return ComplexObject{Type: HashCode(typeName), Fields: map[int32]interface{}{}}
}

// ToDate converts Golang time.Time to Apache Ignite Date
func ToDate(t time.Time) Date {
	t1 := t.UTC()
	t2 := t1.Unix() * 1000
	t2 += int64(t1.Nanosecond()) / int64(time.Millisecond)
	return Date(t2)
}

// Time is Apache Ignite Time type
type Time int64

// ToTime converts Golang time.Time to Apache Ignite Time
func ToTime(t time.Time) Time {
	t1 := t.UTC()
	t2 := time.Date(1970, 1, 1, t1.Hour(), t1.Minute(), t1.Second(), t1.Nanosecond(), time.UTC)
	t3 := t2.Unix() * 1000
	t3 += int64(t2.Nanosecond()) / int64(time.Millisecond)
	return Time(t3)
}

// Flips a UUID buffer into the right order
func uuidFlip(id *uuid.UUID) {
	for i := 3; i >= 0; i-- {
		opp := 7-i
		id[i], id[opp] = id[opp], id[i]
	}
	for i := 3; i >= 0; i-- {
		opp := 15-i
		id[i+8], id[opp] = id[opp], id[i+8]
	}
}

// WriteType writes object type code
func WriteType(w io.Writer, code byte) error {
	return WriteByte(w, code)
}

// WriteByte writes "byte" value
func WriteByte(w io.Writer, v byte) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOByte writes "byte" object value
func WriteOByte(w io.Writer, v byte) error {
	if err := WriteType(w, typeByte); err != nil {
		return err
	}
	return WriteByte(w, v)
}

// WriteShort writes "short" value
func WriteShort(w io.Writer, v int16) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOShort writes "short" object value
func WriteOShort(w io.Writer, v int16) error {
	if err := WriteType(w, typeShort); err != nil {
		return err
	}
	return WriteShort(w, v)
}

// WriteInt writes "int" value
func WriteInt(w io.Writer, v int32) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOInt writes "int" object value
func WriteOInt(w io.Writer, v int32) error {
	if err := WriteType(w, typeInt); err != nil {
		return err
	}
	return WriteInt(w, v)
}

// WriteLong writes "long" value
func WriteLong(w io.Writer, v int64) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOLong writes "long" object value
func WriteOLong(w io.Writer, v int64) error {
	if err := WriteType(w, typeLong); err != nil {
		return err
	}
	return WriteLong(w, v)
}

// WriteFloat writes "float" value
func WriteFloat(w io.Writer, v float32) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOFloat writes "float" object value
func WriteOFloat(w io.Writer, v float32) error {
	if err := WriteType(w, typeFloat); err != nil {
		return err
	}
	return WriteFloat(w, v)
}

// WriteDouble writes "double" value
func WriteDouble(w io.Writer, v float64) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteODouble writes "double" object value
func WriteODouble(w io.Writer, v float64) error {
	if err := WriteType(w, typeDouble); err != nil {
		return err
	}
	return WriteDouble(w, v)
}

// WriteChar writes "char" value
func WriteChar(w io.Writer, v Char) error {
	return binary.Write(w, binary.LittleEndian, int16(v))
}

// WriteOChar writes "char" object value
func WriteOChar(w io.Writer, v Char) error {
	if err := WriteType(w, typeChar); err != nil {
		return err
	}
	return WriteChar(w, v)
}

// WriteBool writes "bool" value
func WriteBool(w io.Writer, v bool) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOBool writes "bool" object value
func WriteOBool(w io.Writer, v bool) error {
	if err := WriteType(w, typeBool); err != nil {
		return err
	}
	return WriteBool(w, v)
}

// WriteOString writes "string" object value
// String is marshalling as object in all cases.
func WriteOString(w io.Writer, v string) error {
	if err := WriteType(w, typeString); err != nil {
		return err
	}
	s := []byte(v)
	if err := WriteInt(w, int32(len(s))); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, s)
}

// WriteOUUID writes "UUID" object value
// UUID is marshaled as object in all cases.
func WriteOUUID(w io.Writer, v uuid.UUID) error {
	if err := WriteType(w, typeUUID); err != nil {
		return err
	}
	uuidFlip(&v)
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteODate writes "Date" object value
func WriteODate(w io.Writer, v Date) error {
	if err := WriteType(w, typeDate); err != nil {
		return err
	}
	return WriteLong(w, int64(v))
}

// WriteBytes writes byte slice
func WriteBytes(w io.Writer, v []byte) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOArrayBytes writes "byte" array object value
func WriteOArrayBytes(w io.Writer, v []byte) error {
	if err := WriteType(w, typeByteArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOArrayShorts writes "short" array object value
func WriteOArrayShorts(w io.Writer, v []int16) error {
	if err := WriteType(w, typeShortArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOArrayInts writes "int" array object value
func WriteOArrayInts(w io.Writer, v []int32) error {
	if err := WriteType(w, typeIntArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOArrayLongs writes "long" array object value
func WriteOArrayLongs(w io.Writer, v []int64) error {
	if err := WriteType(w, typeLongArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOArrayFloats writes "float" array object value
func WriteOArrayFloats(w io.Writer, v []float32) error {
	if err := WriteType(w, typeFloatArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOArrayDoubles writes "double" array object value
func WriteOArrayDoubles(w io.Writer, v []float64) error {
	if err := WriteType(w, typeDoubleArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOArrayChars writes "char" array object value
func WriteOArrayChars(w io.Writer, v []Char) error {
	if err := WriteType(w, typeCharArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	for _, c := range v {
		if err := WriteChar(w, c); err != nil {
			return err
		}
	}
	return nil
}

// WriteOArrayBools writes "Bool" array object value
func WriteOArrayBools(w io.Writer, v []bool) error {
	if err := WriteType(w, typeBoolArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, v)
}

// WriteOArrayOStrings writes "String" array object value
func WriteOArrayOStrings(w io.Writer, v []string) error {
	if err := WriteType(w, typeStringArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	for _, s := range v {
		if err := WriteOString(w, s); err != nil {
			return err
		}
	}
	return nil
}

// WriteOArrayOUUIDs writes "UUID" array object value
func WriteOArrayOUUIDs(w io.Writer, v []uuid.UUID) error {
	if err := WriteType(w, typeUUIDArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	for _, d := range v {
		if err := WriteOUUID(w, d); err != nil {
			return err
		}
	}
	return nil
}

// WriteOArrayODates writes "Date" array object value
func WriteOArrayODates(w io.Writer, v []Date) error {
	if err := WriteType(w, typeDateArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	for _, d := range v {
		if err := WriteODate(w, d); err != nil {
			return err
		}
	}
	return nil
}

// WriteOTimestamp writes "Timestamp" object value
// Timestamp is marshaled as object in all cases.
func WriteOTimestamp(w io.Writer, v time.Time) error {
	if err := WriteType(w, typeTimestamp); err != nil {
		return err
	}
	high := int64(v.Unix() * 1000) // Unix time in milliseconds
	low := v.Nanosecond()
	high += int64(low / int(time.Millisecond))
	low = low % int(time.Millisecond)
	if err := WriteLong(w, high); err != nil {
		return err
	}
	return WriteInt(w, int32(low))
}

// WriteOArrayOTimestamps writes "Timestamp" array object value
func WriteOArrayOTimestamps(w io.Writer, v []time.Time) error {
	if err := WriteType(w, typeTimestampArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	for _, d := range v {
		if err := WriteOTimestamp(w, d); err != nil {
			return err
		}
	}
	return nil
}

// WriteOTime writes "Time" object value
// Time is marshaled as object in all cases.
func WriteOTime(w io.Writer, v Time) error {
	if err := WriteType(w, typeTime); err != nil {
		return err
	}
	return WriteLong(w, int64(v))
}

// WriteOArrayOTimes writes "Time" array object value
func WriteOArrayOTimes(w io.Writer, v []Time) error {
	if err := WriteType(w, typeTimeArray); err != nil {
		return err
	}
	if err := WriteInt(w, int32(len(v))); err != nil {
		return err
	}
	for _, d := range v {
		if err := WriteOTime(w, d); err != nil {
			return err
		}
	}
	return nil
}

// WriteNull writes NULL
func WriteNull(w io.Writer) error {
	return WriteByte(w, typeNULL)
}

// WriteOComplexObject writes complex object
func WriteOComplexObject(w io.Writer, v ComplexObject) error {
	// write type code
	if err := WriteType(w, typeComplexObject); err != nil {
		return err
	}
	// write version
	if err := WriteByte(w, ComplexObjectVersion); err != nil {
		return err
	}
	// write flags
	if err := WriteShort(w, ComplexObjectHasSchema|ComplexObjectUserType); err != nil {
		return err
	}

	// write object type ID
	if err := WriteInt(w, v.Type); err != nil {
		return err
	}

	// prepare schema & content
	schema := &bytes.Buffer{}
	fields := &bytes.Buffer{}
	schemaID := uint32(0x811C9DC5)
	for field, value := range v.Fields {
		fieldID := uint32(field)
		schemaID = schemaID ^ (fieldID & 0xFF)
		schemaID = schemaID * uint32(0x01000193)
		schemaID = schemaID ^ ((fieldID >> 8) & 0xFF)
		schemaID = schemaID * uint32(0x01000193)
		schemaID = schemaID ^ ((fieldID >> 16) & 0xFF)
		schemaID = schemaID * uint32(0x01000193)
		schemaID = schemaID ^ ((fieldID >> 24) & 0xFF)
		schemaID = schemaID * uint32(0x01000193)
		if err := WriteInt(schema, field); err != nil {
			return errors.Wrapf(err, "failed to write field ID with hash %d", field)
		}
		if err := WriteInt(schema, int32(ComplexObjectHeaderLength+fields.Len())); err != nil {
			return errors.Wrapf(err, "failed to write field offset with hash %d", field)
		}
		if err := WriteObject(fields, value); err != nil {
			return errors.Wrapf(err, "failed to write field value with hash %d", field)
		}
	}
	schemaOffset := ComplexObjectHeaderLength + fields.Len()

	// write hash code, Java-style hash of contents without header, necessary for comparisons.
	if err := WriteInt(w, HashCodeForSlice(fields.Bytes())); err != nil {
		return err
	}

	// write length, including header
	if err := WriteInt(w, int32(ComplexObjectHeaderLength+fields.Len()+schema.Len())); err != nil {
		return err
	}

	// write schema Id
	if err := WriteInt(w, int32(schemaID)); err != nil {
		return err
	}

	// write schema offset from the header start, position where fields end.
	if err := WriteInt(w, int32(schemaOffset)); err != nil {
		return err
	}

	// write fields value
	if err := WriteBytes(w, fields.Bytes()); err != nil {
		return err
	}

	// write structure of schema
	return WriteBytes(w, schema.Bytes())
}

// WriteObject writes object
func WriteObject(w io.Writer, o interface{}) error {
	if o == nil {
		return WriteNull(w)
	}

	switch v := o.(type) {
	case byte:
		return WriteOByte(w, v)
	case int16:
		return WriteOShort(w, v)
	case int32:
		return WriteOInt(w, v)
	case int64:
		return WriteOLong(w, v)
	case float32:
		return WriteOFloat(w, v)
	case float64:
		return WriteODouble(w, v)
	case Char:
		return WriteOChar(w, v)
	case bool:
		return WriteOBool(w, v)
	case string:
		return WriteOString(w, v)
	case uuid.UUID:
		return WriteOUUID(w, v)
	case Date:
		return WriteODate(w, v)
	case []byte:
		return WriteOArrayBytes(w, v)
	case []int16:
		return WriteOArrayShorts(w, v)
	case []int32:
		return WriteOArrayInts(w, v)
	case []int64:
		return WriteOArrayLongs(w, v)
	case []float32:
		return WriteOArrayFloats(w, v)
	case []float64:
		return WriteOArrayDoubles(w, v)
	case []Char:
		return WriteOArrayChars(w, v)
	case []bool:
		return WriteOArrayBools(w, v)
	case []string:
		return WriteOArrayOStrings(w, v)
	case []Date:
		return WriteOArrayODates(w, v)
	case []uuid.UUID:
		return WriteOArrayOUUIDs(w, v)
	case time.Time:
		return WriteOTimestamp(w, v)
	case []time.Time:
		return WriteOArrayOTimestamps(w, v)
	case Time:
		return WriteOTime(w, v)
	case []Time:
		return WriteOArrayOTimes(w, v)
	case ComplexObject:
		return WriteOComplexObject(w, v)
	case *ComplexObject:
		return WriteOComplexObject(w, *v)
	default:
		return errors.Errorf("unsupported object type: %s", reflect.TypeOf(v).Name())
	}
}

// ReadByte reads "byte" value
func ReadByte(r io.Reader) (byte, error) {
	var v byte
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

// ReadShort reads "short" value
func ReadShort(r io.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

// ReadInt reads "int" value
func ReadInt(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

// ReadLong reads "long" value
func ReadLong(r io.Reader) (int64, error) {
	var v int64
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

// ReadFloat reads "float" value
func ReadFloat(r io.Reader) (float32, error) {
	var v float32
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

// ReadDouble reads "Double" value
func ReadDouble(r io.Reader) (float64, error) {
	var v float64
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

// ReadChar reads "char" value
func ReadChar(r io.Reader) (Char, error) {
	var v int16
	err := binary.Read(r, binary.LittleEndian, &v)
	return Char(v), err
}

// ReadBool reads "bool" value
func ReadBool(r io.Reader) (bool, error) {
	v, err := ReadByte(r)
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
func ReadString(r io.Reader) (string, error) {
	l, err := ReadInt(r)
	if err != nil {
		return "", err
	}
	if l > 0 {
		s := make([]byte, l)
		if err = binary.Read(r, binary.LittleEndian, &s); err != nil {
			return "", err
		}
		return string(s), nil
	}
	return "", nil
}

// ReadOString reads "string" object value or NULL (returns "")
func ReadOString(r io.Reader) (string, error) {
	t, err := ReadByte(r)
	if err != nil {
		return "", err
	}
	switch t {
	case typeNULL:
		return "", nil
	case typeString:
		v, err := ReadString(r)
		return v, err
	default:
		return "", errors.Errorf("invalid type (expected %d, but got %d)", typeString, t)
	}
}

// ReadUUID reads "UUID" object value
func ReadUUID(r io.Reader) (uuid.UUID, error) {
	var o uuid.UUID
	err := binary.Read(r, binary.LittleEndian, &o)
	uuidFlip(&o)
	return o, err
}

// ReadDate reads "Date" object value
func ReadDate(r io.Reader) (time.Time, error) {
	v, err := ReadLong(r)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(v)/1000, (int64(v)%1000)*int64(time.Millisecond)).UTC(), nil
}

// ReadArrayBytes reads "byte" array value
func ReadArrayBytes(r io.Reader) ([]byte, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]byte, l)
	if l > 0 {
		err = binary.Read(r, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayShorts reads "short" array value
func ReadArrayShorts(r io.Reader) ([]int16, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]int16, l)
	if l > 0 {
		err = binary.Read(r, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayInts reads "int" array value
func ReadArrayInts(r io.Reader) ([]int32, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]int32, l)
	if l > 0 {
		err = binary.Read(r, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayLongs reads "long" array value
func ReadArrayLongs(r io.Reader) ([]int64, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]int64, l)
	if l > 0 {
		err = binary.Read(r, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayFloats reads "float" array value
func ReadArrayFloats(r io.Reader) ([]float32, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]float32, l)
	if l > 0 {
		err = binary.Read(r, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayDoubles reads "double" array value
func ReadArrayDoubles(r io.Reader) ([]float64, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]float64, l)
	if l > 0 {
		err = binary.Read(r, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayChars reads "char" array value
func ReadArrayChars(r io.Reader) ([]Char, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]Char, l)
	for i := 0; i < int(l); i++ {
		if b[i], err = ReadChar(r); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// ReadArrayBools reads "bool" array value
func ReadArrayBools(r io.Reader) ([]bool, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]bool, l)
	if l > 0 {
		err = binary.Read(r, binary.LittleEndian, &b)
	}
	return b, err
}

// ReadArrayOStrings reads "String" array value
func ReadArrayOStrings(r io.Reader) ([]string, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]string, l)
	for i := 0; i < int(l); i++ {
		if b[i], err = ReadOString(r); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// ReadArrayOUUIDs reads "UUID" array value
func ReadArrayOUUIDs(r io.Reader) ([]uuid.UUID, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]uuid.UUID, l)
	for i := 0; i < int(l); i++ {
		o, err := ReadObject(r)
		if err != nil {
			return nil, err
		}
		b[i] = o.(uuid.UUID)
	}
	return b, nil
}

// ReadArrayODates reads "Date" array value
func ReadArrayODates(r io.Reader) ([]time.Time, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]time.Time, l)
	for i := 0; i < int(l); i++ {
		o, err := ReadObject(r)
		if err != nil {
			return nil, err
		}
		b[i] = o.(time.Time)
	}
	return b, nil
}

// ReadArrayBinaryObject reads "binary object" value wrapped by array
func ReadArrayBinaryObject(r io.Reader) (interface{}, error) {
	// read byte array size
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}

	// read byte array
	b := make([]byte, l)
	if err = binary.Read(r, binary.LittleEndian, &b); err != nil {
		return nil, err
	}

	// read object offset
	o, err := ReadInt(r)
	if err != nil {
		return nil, err
	}

	// read object
	buf := bytes.NewBuffer(b[int(o):])
	return ReadObject(buf)
}

// ReadTimestamp reads "Timestamp" object value
func ReadTimestamp(r io.Reader) (time.Time, error) {
	high, err := ReadLong(r)
	if err != nil {
		return time.Time{}, err
	}
	low, err := ReadInt(r)
	if err != nil {
		return time.Time{}, err
	}
	low = int32((high%1000)*int64(time.Millisecond)) + low
	high = high / 1000
	return time.Unix(high, int64(low)).UTC(), nil
}

// ReadArrayOTimestamps reads "Timestamp" array value
func ReadArrayOTimestamps(r io.Reader) ([]time.Time, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]time.Time, l)
	for i := 0; i < int(l); i++ {
		o, err := ReadObject(r)
		if err != nil {
			return nil, err
		}
		b[i] = o.(time.Time)
	}
	return b, nil
}

// ReadTime reads "Time" object value
func ReadTime(r io.Reader) (time.Time, error) {
	v, err := ReadLong(r)
	if err != nil {
		return time.Time{}, err
	}
	t := time.Unix(int64(v)/1000, (int64(v)%1000)*int64(time.Millisecond)).UTC()
	return time.Date(1, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC), nil
}

// ReadArrayOTimes reads "Time" array value
func ReadArrayOTimes(r io.Reader) ([]time.Time, error) {
	l, err := ReadInt(r)
	if err != nil {
		return nil, err
	}
	b := make([]time.Time, l)
	for i := 0; i < int(l); i++ {
		o, err := ReadObject(r)
		if err != nil {
			return nil, err
		}
		b[i] = o.(time.Time)
	}
	return b, nil
}

// ReadComplexObject reads "complex object" value
func ReadComplexObject(r io.Reader) (ComplexObject, error) {
	// read version, always 1
	ver, err := ReadByte(r)
	if err != nil {
		return ComplexObject{}, err
	}
	if ver != ComplexObjectVersion {
		return ComplexObject{}, errors.Errorf("invalid complex object version %d, but expected %d", ver, ComplexObjectVersion)
	}

	// read flags
	flags, err := ReadShort(r)
	if err != nil {
		return ComplexObject{}, err
	}

	// read Type id, Java-style hash code of the type name
	typeID, err := ReadInt(r)
	if err != nil {
		return ComplexObject{}, err
	}

	// read hash code, Java-style hash of contents without header, necessary for comparisons
	if _, err = ReadInt(r); err != nil {
		return ComplexObject{}, err
	}

	// read length, including header
	size, err := ReadInt(r)
	if err != nil {
		return ComplexObject{}, err
	}

	// read schema Id
	if _, err = ReadInt(r); err != nil {
		return ComplexObject{}, err
	}

	// read schema offset from the header start, position where fields end
	schemaOffset, err := ReadInt(r)
	if err != nil {
		return ComplexObject{}, err
	}

	// read fields
	fields := make([]byte, schemaOffset-ComplexObjectHeaderLength)
	if err = binary.Read(r, binary.LittleEndian, &fields); err != nil {
		return ComplexObject{}, err
	}

	// read field schemas and data
	left := size - schemaOffset
	var step int32
	if flags&ComplexObjectOffsetOneByte != 0 {
		step = 1
	} else if flags&ComplexObjectOffsetTwoBytes != 0 {
		step = 2
	} else {
		step = 4
	}
	i := int32(1)
	c := ComplexObject{Type: typeID, Fields: map[int32]interface{}{}}
	for left > 0 {
		var fieldID int32
		if flags&ComplexObjectCompactFooter == 0 {
			// get field ID
			fieldID, err = ReadInt(r)
			if err != nil {
				return ComplexObject{}, errors.Wrapf(err, "failed to read field ID with index %d", i)
			}
			left -= 4
		} else {
			fieldID = i
		}

		// get field offset
		var fieldOffset int
		switch step {
		case 1:
			offset, err := ReadByte(r)
			if err != nil {
				return ComplexObject{}, errors.Wrapf(err, "failed to read field offset with index %d", i)
			}
			fieldOffset = int(offset)
		case 2:
			offset, err := ReadShort(r)
			if err != nil {
				return ComplexObject{}, errors.Wrapf(err, "failed to read field offset with index %d", i)
			}
			fieldOffset = int(offset)
		default:
			offset, err := ReadInt(r)
			if err != nil {
				return ComplexObject{}, errors.Wrapf(err, "failed to read field offset with index %d", i)
			}
			fieldOffset = int(offset)
		}
		left -= step

		// read field data
		field := bytes.NewBuffer(fields[fieldOffset-ComplexObjectHeaderLength:])
		o, err := ReadObject(field)
		if err != nil {
			return ComplexObject{}, errors.Wrapf(err, "failed to read field data with index %d", i)
		}
		c.Fields[fieldID] = o
		i++
	}

	return c, nil
}

// ReadObject read object
func ReadObject(r io.Reader) (interface{}, error) {
	t, err := ReadByte(r)
	if err != nil {
		return nil, err
	}

	switch t {
	case typeByte:
		return ReadByte(r)
	case typeShort:
		return ReadShort(r)
	case typeInt:
		return ReadInt(r)
	case typeLong:
		return ReadLong(r)
	case typeFloat:
		return ReadFloat(r)
	case typeDouble:
		return ReadDouble(r)
	case typeChar:
		return ReadChar(r)
	case typeBool:
		return ReadBool(r)
	case typeString:
		return ReadString(r)
	case typeUUID:
		return ReadUUID(r)
	case typeDate:
		return ReadDate(r)
	case typeByteArray:
		return ReadArrayBytes(r)
	case typeShortArray:
		return ReadArrayShorts(r)
	case typeIntArray:
		return ReadArrayInts(r)
	case typeLongArray:
		return ReadArrayLongs(r)
	case typeFloatArray:
		return ReadArrayFloats(r)
	case typeDoubleArray:
		return ReadArrayDoubles(r)
	case typeCharArray:
		return ReadArrayChars(r)
	case typeBoolArray:
		return ReadArrayBools(r)
	case typeStringArray:
		return ReadArrayOStrings(r)
	case typeDateArray:
		return ReadArrayODates(r)
	case typeBinaryObjectArray:
		return ReadArrayBinaryObject(r)
	case typeUUIDArray:
		return ReadArrayOUUIDs(r)
	case typeTimestamp:
		return ReadTimestamp(r)
	case typeTimestampArray:
		return ReadArrayOTimestamps(r)
	case typeTime:
		return ReadTime(r)
	case typeTimeArray:
		return ReadArrayOTimes(r)
	case typeNULL:
		return nil, nil
	case typeComplexObject:
		return ReadComplexObject(r)
	default:
		return nil, errors.Errorf("unsupported object type: %d", t)
	}
}
