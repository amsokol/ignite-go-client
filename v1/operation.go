package ignite

// Operation allows to prepare operation to execute
type Operation interface {
	Code() int16
	UID() int64
	Data() []byte

	Write(data ...interface{}) error
}
