package ignite

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
	typeTimestamp byte = 33
	// TODO: Timestamp Array = 34
	typeTime byte = 36
	// TODO: Time Array = 37
	typeNULL byte = 101
)

// Char is Apache Ignite "char" type
type Char rune
