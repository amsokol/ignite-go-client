package ignite

const (
	// Supported standard types and their type codes are as follows:
	typeByte   = 1
	typeShort  = 2
	typeInt    = 3
	typeLong   = 4
	typeFloat  = 5
	typeDouble = 6
	typeChar   = 7
	typeBool   = 8
	typeString = 9
	typeUUID   = 10
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
	// TODO: typeDate        = 11
	typeByteArray = 12
	// TODO: typeShortArray  = 13
	// TODO: typeIntArray    = 14
	// TODO: typeLongArray   = 15
	// TODO: typeFloatArray  = 16
	// TODO: typeDoubleArray = 17
	// TODO: typeCharArray   = 18
	// TODO: typeBoolArray   = 19
	// TODO: typeStringArray = 20
	// TODO: typeUUIDArray   = 21
	// TODO: typeDateArray   = 22
	// TODO: Object array = 23
	// TODO: Collection = 24
	// TODO: Map = 25
	// TODO: Enum = 28
	// TODO: Enum Array = 29
	// TODO: Decimal = 30
	// TODO: Decimal Array = 31
	typeTimestamp byte = 33
	// TODO: Timestamp Array = 34
	// TODO: typeTime byte = 36
	// TODO: Time Array = 37
	typeNULL byte = 101
)

// Char is Apache Ignite "char" type
type Char rune
