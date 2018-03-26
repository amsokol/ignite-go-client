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

	// Key-Value Queries
	opCacheGet               = 1000
	opCachePut               = 1001
	opCachePutIfAbsent       = 1002
	opCacheGetAll            = 1003
	opCachePutAll            = 1004
	opCacheGetAndPut         = 1005
	opCacheGetAndReplace     = 1006
	opCacheGetAndRemove      = 1007
	opCacheGetAndPutIfAbsent = 1008
	opCacheReplace           = 1009
	opCacheReplaceIfEquals   = 1010
	opCacheContainsKey       = 1011
	opCacheContainsKeys      = 1012
	opCacheClear             = 1013
	opCacheClearKey          = 1014
	opCacheClearKeys         = 1015
	opCacheRemoveKey         = 1016
	opCacheRemoveIfEquals    = 1017
	opCacheRemoveKeys        = 1018
	opCacheRemoveAll         = 1019
	opCacheGetSize           = 1020
)

// Operation allows to prepare operation to execute
type Operation struct {
	Code   int16
	UID    int64
	Prefix bytes.Buffer
	Data   bytes.Buffer
}

// WritePrefix writes promitives to the prefix buffer.
// Prefix is sent to the server before data.
func (o *Operation) WritePrefix(data ...interface{}) error {
	return writePrimitives(&o.Prefix, data...)
}

// WritePrimitives writes primitives to the data buffer
func (o *Operation) WritePrimitives(data ...interface{}) error {
	return writePrimitives(&o.Data, data...)
}

// WriteObjects writes objects to the data buffer
func (o *Operation) WriteObjects(data ...interface{}) error {
	return writeObjects(&o.Data, data...)
}
