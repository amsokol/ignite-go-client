package ignite

import (
	"bytes"
)

const (
	// Cache Configuration
	opCacheGetNames                     = 1050
	opCacheCreateWithName               = 1051
	opCacheGetOrCreateWithName          = 1052
	opCacheCreateWithConfiguration      = 1053
	opCacheGetOrCreateWithConfiguration = 1054
	opCacheGetConfiguration             = 1055
	opCacheDestroy                      = 1056
)

// Operation allows to prepare operation to execute
type Operation struct {
	Code   int16
	UID    int64
	Prefix bytes.Buffer
	Data   bytes.Buffer
}

// WritePrefix writes data to the prefix buffer.
// Prefix is sent to the server before data.
func (o *Operation) WritePrefix(data ...interface{}) error {
	return write(&o.Prefix, data...)
}

// Write writes data to the data buffer
func (o *Operation) Write(data ...interface{}) error {
	return write(&o.Data, data...)
}
