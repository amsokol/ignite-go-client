package ignite

import (
	"bytes"
	"fmt"
	"io"
	"net"
)

const (
	// StatusSuccess means success
	StatusSuccess = 0
)

// Client is interface to communicate with Apache Ignite cluster.
// Client is not thread-safe.
type Client interface {
	// Exec executes request with primitives.
	// code - code of operation.
	// uid - request ID.
	// primitives - primitives to send.
	// Returns:
	// Response, nil in case of success.
	// Empty Response, error object in case of error.
	Exec(code int16, uid int64, primitives ...interface{}) (Response, error)

	// Prepare returns Operation.
	// Arguments:
	// code - code of operation.
	// uid - request ID.
	// Operation is not thread-safe.
	Prepare(code int16, uid int64) Operation

	// Call executes Operation
	// Arguments:
	// o - Operation to execute.
	// Returns:
	// Response, nil in case of success.
	// Empty Response, error object in case of error.
	Call(o Operation) (Response, error)

	// Begin starts request by writing data directly to connection with server.
	// Arguments:
	// length - length in bytes of request message.
	// code - code of operation.
	// uid - request ID.
	// Returns:
	// nil in case of success.
	// error object in case of error.
	Begin(length int32, code int16, uid int64) error

	// Write writes primitives directly to connection with server.
	// Arguments:
	// primitives - primitives to write.
	// Returns:
	// nil in case of success.
	// error object in case of error.
	Write(primitives ...interface{}) error

	// Commit finishes the request and returns response from server.
	// Returns:
	// Response, nil in case of success.
	// Empty Response, error object in case of error.
	Commit() (Response, error)

	// Close closes connection.
	// Returns:
	// nil in case of success.
	// error object in case of error.
	Close() error

	// Cache Configuration methods
	// See for details:
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations
	// Each method returns Apache Ignite error code if you provide reference to status arg.
	// Provide nil instead of reference if you don't need it.

	// CacheCreateWithName Creates a cache with a given name.
	// Cache template can be applied if there is a '*' in the cache name.
	CacheCreateWithName(cache string, status *int32) error

	// CacheGetOrCreateWithName creates a cache with a given name.
	// Cache template can be applied if there is a '*' in the cache name.
	// Does nothing if the cache exists.
	CacheGetOrCreateWithName(cache string, status *int32) error

	// CacheGetNames returns existing cache names.
	CacheGetNames(status *int32) ([]string, error)

	// CacheGetConfiguration gets configuration for the given cache.
	CacheGetConfiguration(cache string, flag byte, status *int32) (*CacheConfiguration, error)

	// CacheCreateWithConfiguration creates cache with provided configuration.
	// An error is returned if the name is already in use.
	CacheCreateWithConfiguration(cc *CacheConfigurationRefs, status *int32) error

	// CacheDestroy destroys cache with a given name.
	CacheDestroy(cache string, status *int32) error

	// Key-Value Queries
	// See for details:
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations
	// Each method returns Apache Ignite error code if you provide reference to status arg.
	// Provide nil instead of reference if you don't need it.

	// CachePut puts a value with a given key to cache (overwriting existing value if any).
	CachePut(cache string, binary bool, key interface{}, value interface{}, status *int32) error

	// CachePutAll puts a value with a given key to cache (overwriting existing value if any).
	CachePutAll(cache string, binary bool, data map[interface{}]interface{}, status *int32) error

	// CacheGet retrieves a value from cache by key.
	CacheGet(cache string, binary bool, key interface{}, status *int32) (interface{}, error)

	// CacheGetAll retrieves multiple key-value pairs from cache.
	CacheGetAll(cache string, binary bool, keys []interface{}, status *int32) (map[interface{}]interface{}, error)

	// CacheContainsKey returns a value indicating whether given key is present in cache.
	CacheContainsKey(cache string, binary bool, key interface{}, status *int32) (bool, error)

	// CacheContainsKeys returns a value indicating whether all given keys are present in cache.
	CacheContainsKeys(cache string, binary bool, keys []interface{}, status *int32) (bool, error)

	// CacheGetAndPut puts a value with a given key to cache, and returns the previous value for that key.
	CacheGetAndPut(cache string, binary bool, key interface{}, value interface{}, status *int32) (interface{}, error)

	// CacheGetAndReplace puts a value with a given key to cache, returning previous value for that key,
	// if and only if there is a value currently mapped for that key.
	CacheGetAndReplace(cache string, binary bool, key interface{}, value interface{}, status *int32) (interface{}, error)

	// CacheGetAndRemove removes the cache entry with specified key, returning the value.
	CacheGetAndRemove(cache string, binary bool, key interface{}, status *int32) (interface{}, error)

	// CachePutIfAbsent puts a value with a given key to cache only if the key does not already exist.
	CachePutIfAbsent(cache string, binary bool, key interface{}, value interface{}, status *int32) (bool, error)

	// CacheGetAndPutIfAbsent puts a value with a given key to cache only if the key does not already exist.
	CacheGetAndPutIfAbsent(cache string, binary bool, key interface{}, value interface{}, status *int32) (interface{}, error)

	// CacheReplace puts a value with a given key to cache only if the key already exists.
	CacheReplace(cache string, binary bool, key interface{}, value interface{}, status *int32) (bool, error)

	// CacheReplaceIfEquals puts a value with a given key to cache only if
	// the key already exists and value equals provided value.
	CacheReplaceIfEquals(cache string, binary bool, key interface{}, valueCompare interface{}, valueNew interface{}, status *int32) (bool, error)

	// CacheClear clears the cache without notifying listeners or cache writers.
	CacheClear(cache string, binary bool, status *int32) error

	// CacheClearKey clears the cache key without notifying listeners or cache writers.
	CacheClearKey(cache string, binary bool, key interface{}, status *int32) error

	// CacheClearKeys clears the cache keys without notifying listeners or cache writers.
	CacheClearKeys(cache string, binary bool, keys []interface{}, status *int32) error
}

type client struct {
	conn net.Conn

	Client
}

// Close closes connection.
// Returns:
// nil in case of success.
// error object in case of error.
func (c *client) Close() error {
	if c.conn != nil {
		defer func() { c.conn = nil }()
		return c.conn.Close()
	}
	return nil
}

// Exec executes request with primitives.
// code - code of operation.
// uid - request ID.
// primitives - primitives to send.
// Returns:
// Response, nil in case of success.
// Empty Response, error object in case of error.
func (c *client) Exec(code int16, uid int64, primitives ...interface{}) (Response, error) {
	o := c.Prepare(code, uid)
	// write data
	if err := o.WritePrimitives(primitives...); err != nil {
		return Response{}, fmt.Errorf("failed to write request primitives to operation: %s", err.Error())
	}
	return c.Call(o)
}

// Prepare returns Operation.
// Arguments:
// code - code of operation.
// uid - request ID.
// Operation is not thread-safe.
func (c *client) Prepare(code int16, uid int64) Operation {
	return Operation{Code: code, UID: uid, Prefix: &bytes.Buffer{}, Data: &bytes.Buffer{}}
}

// Call executes Operation
// Arguments:
// o - Operation to execute.
// Returns:
// Response, nil in case of success.
// Empty Response, error object in case of error
func (c *client) Call(o Operation) (Response, error) {
	// send request header
	if err := c.Begin(int32(2+8+o.Prefix.Len()+o.Data.Len()), o.Code, o.UID); err != nil {
		return Response{}, fmt.Errorf("failed to send request header: %s", err.Error())
	}
	if o.Prefix.Len() > 0 {
		// send request prefix of body
		if err := c.Write(o.Prefix.Bytes()); err != nil {
			return Response{}, fmt.Errorf("failed to send request prefix of body: %s", err.Error())
		}
	}
	if o.Data.Len() > 0 {
		// send request body
		if err := c.Write(o.Data.Bytes()); err != nil {
			return Response{}, fmt.Errorf("failed to send request body: %s", err.Error())
		}
	}
	return c.Commit()
}

// Begin starts request by writing data directly to connection with server.
// Arguments:
// length - length in bytes of request message.
// code - code of operation.
// uid - request ID.
// Returns:
// nil in case of success.
// error object in case of error.
func (c *client) Begin(length int32, code int16, uid int64) error {
	return writePrimitives(c.conn, length, code, uid)
}

// Write writes primitives directly to connection with server.
// Arguments:
// primitives - primitives to write.
// Returns:
// nil in case of success.
// error object in case of error.
func (c *client) Write(primitives ...interface{}) error {
	return writePrimitives(c.conn, primitives...)
}

// Commit finishes the request and returns response from server.
// Returns:
// Response, nil in case of success.
// Empty Response, error object in case of error.
func (c *client) Commit() (Response, error) {
	var r Response

	// read response message length
	if err := readPrimitives(c.conn, &r.Len); err != nil {
		return r, fmt.Errorf("failed to read response message length: %s", err.Error())
	}

	// read response message
	b := make([]byte, r.Len, r.Len)
	if err := readPrimitives(c.conn, &b); err != nil {
		return r, fmt.Errorf("failed to read response message: %s", err.Error())
	}
	r.Data = bytes.NewReader(b)

	// read response header
	if err := r.ReadPrimitives(&r.UID, &r.Status); err != nil {
		return r, fmt.Errorf("failed to read response header: %s", err.Error())
	}

	if r.Status != StatusSuccess {
		// Response status
		if err := r.ReadPrimitives(&r.Message); err != nil {
			return r, fmt.Errorf("failed to read error message: %s", err.Error())
		}
	}
	return r, nil
}

// NewClient100 connects to the Apache Ignite cluster by protocol version v1.0.0
func NewClient100(network, address string) (Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %s", err.Error())
	}
	if err = handshake(conn, 1, 0, 0); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to make handshake: %s", err.Error())
	}
	return &client{conn: conn}, nil
}

// handshake - besides socket connection, the thin client protocol requires a connection handshake to ensure
// that client and server versions are compatible. Note that handshake must be the first message
// after connection establishment.
func handshake(rw io.ReadWriter, major int16, minor int16, patch int16) error {
	// Send handshake request
	if err := writePrimitives(rw,
		// Message length
		int32(8),
		// Handshake operation
		byte(1),
		// Protocol version, e.g. 1,0,0
		major, minor, patch,
		// Client code
		byte(2),
	); err != nil {
		return fmt.Errorf("failed to send handshake request: %s", err.Error())
	}

	// Receive handshake response
	var length int32
	var res byte
	if err := readPrimitives(rw, &length, &res); err != nil {
		return fmt.Errorf("failed to read handshake response (length and result): %s", err.Error())
	}
	if res != 1 {
		var msg string
		if err := readPrimitives(rw, &major, &minor, &patch, &msg); err != nil {
			return fmt.Errorf("failed to read handshake response (supported protocol version and error message): %s",
				err.Error())
		}
		return fmt.Errorf("handshake failed, code=%d, message='%s', supported protocol version is v%d.%d.%d",
			res, msg, major, minor, patch)
	}

	return nil
}
