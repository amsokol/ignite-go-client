package ignite

import (
	"bytes"
)

// OperationCode is operation code type
type OperationCode = int16

const (
	// Cache Configuration
	OpCacheGetNames                     = 1050
	OpCacheCreateWithName               = 1051
	OpCacheGetOrCreateWithName          = 1052
	OpCacheCreateWithConfiguration      = 1053
	OpCacheGetOrCreateWithConfiguration = 1054
	OpCacheGetConfiguration             = 1055
	OpCacheDestroy                      = 1056

	// Key-Value Queries
	OpCacheGet               = 1000
	OpCachePut               = 1001
	OpCachePutIfAbsent       = 1002
	OpCacheGetAll            = 1003
	OpCachePutAll            = 1004
	OpCacheGetAndPut         = 1005
	OpCacheGetAndReplace     = 1006
	OpCacheGetAndRemove      = 1007
	OpCacheGetAndPutIfAbsent = 1008
	OpCacheReplace           = 1009
	OpCacheReplaceIfEquals   = 1010
	OpCacheContainsKey       = 1011
	OpCacheContainsKeys      = 1012
	OpCacheClear             = 1013
	OpCacheClearKey          = 1014
	OpCacheClearKeys         = 1015
	OpCacheRemoveKey         = 1016
	OpCacheRemoveIfEquals    = 1017
	OpCacheRemoveKeys        = 1018
	OpCacheRemoveAll         = 1019
	OpCacheGetSize           = 1020

	// SQL and Scan Queries
	OpQuerySQL                    = 2002
	OpQuerySQLCursorGetPage       = 2003
	OpQuerySQLFields              = 2004
	OpQuerySQLFieldsCursorGetPage = 2005
	OpQueryScan                   = 2000
	OpQueryScanCursorGetPage      = 2001
	OpResourceClose               = 0
)

// Operation allows to prepare operation to execute
type Operation struct {
	Code   OperationCode
	UID    int64
	Prefix *bytes.Buffer
	Data   *bytes.Buffer
}

// WritePrefix writes promitives to the prefix buffer.
// Prefix is sent to the server before data.
func (o *Operation) WritePrefix(data ...interface{}) error {
	return writePrimitives(o.Prefix, data...)
}

// WritePrimitives writes primitives to the data buffer
func (o *Operation) WritePrimitives(data ...interface{}) error {
	return writePrimitives(o.Data, data...)
}

// WriteObjects writes objects to the data buffer
func (o *Operation) WriteObjects(data ...interface{}) error {
	return writeObjects(o.Data, data...)
}
