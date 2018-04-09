package ignite

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/amsokol/ignite-go-client/binary/errors"
	"github.com/amsokol/ignite-go-client/debug"
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
	Exec(code int16, primitives ...interface{}) (Response, error)

	// Prepare returns Operation.
	// Arguments:
	// code - code of operation.
	// uid - request ID.
	// Operation is not thread-safe.
	Prepare(code int16) Operation

	// Call executes Operation
	// Arguments:
	// o - Operation to execute.
	// Returns:
	// Response, nil in case of success.
	// Empty Response, error object in case of error.
	Call(o Operation) (Response, error)

	// begin starts request by writing data directly to connection with server.
	// Arguments:
	// length - length in bytes of request message.
	// code - code of operation.
	// uid - request ID.
	// Returns:
	// nil in case of success.
	// error object in case of error.
	begin(length int32, code int16, uid int64) error

	// write writes primitives directly to connection with server.
	// Arguments:
	// primitives - primitives to write.
	// Returns:
	// nil in case of success.
	// error object in case of error.
	write(primitives ...interface{}) error

	// commit finishes the request and returns response from server.
	// Returns:
	// Response, nil in case of success.
	// Empty Response, error object in case of error.
	commit() (Response, error)

	// IsConnected return true if connection to the cluster is active
	IsConnected() bool

	// Close closes connection.
	// Returns:
	// nil in case of success.
	// error object in case of error.
	Close() error

	// Cache Configuration methods
	// See for details:
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations

	// CacheCreateWithName Creates a cache with a given name.
	// Cache template can be applied if there is a '*' in the cache name.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_create_with_name
	CacheCreateWithName(cache string) error

	// CacheGetOrCreateWithName creates a cache with a given name.
	// Cache template can be applied if there is a '*' in the cache name.
	// Does nothing if the cache exists.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_get_or_create_with_name
	CacheGetOrCreateWithName(cache string) error

	// CacheGetNames returns existing cache names.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_get_names
	CacheGetNames() ([]string, error)

	// CacheGetConfiguration gets configuration for the given cache.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_get_configuration
	CacheGetConfiguration(cache string, flag byte) (*CacheConfiguration, error)

	// CacheCreateWithConfiguration creates cache with provided configuration.
	// An error is returned if the name is already in use.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_create_with_configuration
	CacheCreateWithConfiguration(cc *CacheConfigurationRefs) error

	// CacheGetOrCreateWithConfiguration creates cache with provided configuration.
	// Does nothing if the name is already in use.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_get_or_create_with_configuration
	CacheGetOrCreateWithConfiguration(cc *CacheConfigurationRefs) error

	// CacheDestroy destroys cache with a given name.
	// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_destroy
	CacheDestroy(cache string) error

	// Key-Value Queries
	// See for details:
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations

	// CachePut puts a value with a given key to cache (overwriting existing value if any).
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_put
	CachePut(cache string, binary bool, key interface{}, value interface{}) error

	// CachePutAll puts a value with a given key to cache (overwriting existing value if any).
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_put_all
	CachePutAll(cache string, binary bool, data map[interface{}]interface{}) error

	// CacheGet retrieves a value from cache by key.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get
	CacheGet(cache string, binary bool, key interface{}) (interface{}, error)

	// CacheGetAll retrieves multiple key-value pairs from cache.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get_all
	CacheGetAll(cache string, binary bool, keys []interface{}) (map[interface{}]interface{}, error)

	// CacheContainsKey returns a value indicating whether given key is present in cache.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_contains_key
	CacheContainsKey(cache string, binary bool, key interface{}) (bool, error)

	// CacheContainsKeys returns a value indicating whether all given keys are present in cache.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_contains_keys
	CacheContainsKeys(cache string, binary bool, keys []interface{}) (bool, error)

	// CacheGetAndPut puts a value with a given key to cache, and returns the previous value for that key.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get_and_put
	CacheGetAndPut(cache string, binary bool, key interface{}, value interface{}) (interface{}, error)

	// CacheGetAndReplace puts a value with a given key to cache, returning previous value for that key,
	// if and only if there is a value currently mapped for that key.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get_and_replace
	CacheGetAndReplace(cache string, binary bool, key interface{}, value interface{}) (interface{}, error)

	// CacheGetAndRemove removes the cache entry with specified key, returning the value.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get_and_remove
	CacheGetAndRemove(cache string, binary bool, key interface{}) (interface{}, error)

	// CachePutIfAbsent puts a value with a given key to cache only if the key does not already exist.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_put_if_absent
	CachePutIfAbsent(cache string, binary bool, key interface{}, value interface{}) (bool, error)

	// CacheGetAndPutIfAbsent puts a value with a given key to cache only if the key does not already exist.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get_and_put_if_absent
	CacheGetAndPutIfAbsent(cache string, binary bool, key interface{}, value interface{}) (interface{}, error)

	// CacheReplace puts a value with a given key to cache only if the key already exists.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_replace
	CacheReplace(cache string, binary bool, key interface{}, value interface{}) (bool, error)

	// CacheReplaceIfEquals puts a value with a given key to cache only if
	// the key already exists and value equals provided value.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_replace_if_equals
	CacheReplaceIfEquals(cache string, binary bool, key interface{}, valueCompare interface{}, valueNew interface{}) (bool, error)

	// CacheClear clears the cache without notifying listeners or cache writers.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_clear
	CacheClear(cache string, binary bool) error

	// CacheClearKey clears the cache key without notifying listeners or cache writers.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_clear_key
	CacheClearKey(cache string, binary bool, key interface{}) error

	// CacheClearKeys clears the cache keys without notifying listeners or cache writers.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_clear_keys
	CacheClearKeys(cache string, binary bool, keys []interface{}) error

	// CacheRemoveKey removes an entry with a given key, notifying listeners and cache writers.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_remove_key
	CacheRemoveKey(cache string, binary bool, key interface{}) (bool, error)

	// CacheRemoveIfEquals removes an entry with a given key if provided value is equal to actual value,
	// notifying listeners and cache writers.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_remove_if_equals
	CacheRemoveIfEquals(cache string, binary bool, key interface{}, value interface{}) (bool, error)

	// CacheGetSize gets the number of entries in cache.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get_size
	CacheGetSize(cache string, binary bool, count int, modes []byte) (int64, error)

	// CacheRemoveKeys removes entries with given keys, notifying listeners and cache writers.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_remove_keys
	CacheRemoveKeys(cache string, binary bool, keys []interface{}) error

	// CacheRemoveAll destroys cache with a given name.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_remove_all
	CacheRemoveAll(cache string, binary bool) error

	// SQL and Scan Queries
	// See for details:
	// https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations

	// QuerySQL executes an SQL query over data stored in the cluster. The query returns the whole record (key and value).
	// https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations#section-op_query_sql
	QuerySQL(cache string, binary bool, data QuerySQLData) (QuerySQLResult, error)

	// QuerySQLCursorGetPage retrieves the next SQL query cursor page by cursor id from QuerySQL.
	// https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations#section-op_query_sql_cursor_get_page
	QuerySQLCursorGetPage(id int64) (QuerySQLPage, error)

	// QuerySQLFields performs SQL fields query.
	// https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations#section-op_query_sql_fields
	QuerySQLFields(cache string, binary bool, data QuerySQLFieldsData) (QuerySQLFieldsResult, error)

	// QuerySQLFieldsCursorGetPage retrieves the next query result page by cursor id from QuerySQLFields.
	// https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations#section-op_query_sql_fields_cursor_get_page
	QuerySQLFieldsCursorGetPage(id int64, fieldCount int) (QuerySQLFieldsPage, error)

	// ResourceClose closes a resource, such as query cursor.
	// https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations#section-op_resource_close
	ResourceClose(id int64) error
}

type client struct {
	debugID string
	conn    net.Conn
	lock    sync.Mutex

	Client
}

// IsConnected return true if connection to the cluster is active
func (c *client) IsConnected() bool {
	return c.conn != nil
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
func (c *client) Exec(code OperationCode, primitives ...interface{}) (Response, error) {
	// prepare operation
	o := c.Prepare(code)

	// write data
	if err := o.WritePrimitives(primitives...); err != nil {
		return Response{}, errors.Wrapf(err, "failed to write operation request primitives")
	}

	// execute operation
	return c.Call(o)
}

// Prepare returns Operation.
// Arguments:
// code - code of operation.
// uid - request ID.
// Operation is not thread-safe.
func (c *client) Prepare(code OperationCode) Operation {
	return Operation{Code: code, UID: rand.Int63(), Prefix: &bytes.Buffer{}, Data: &bytes.Buffer{}}
}

// Call executes Operation
// Arguments:
// o - Operation to execute.
// Returns:
// Response, nil in case of success.
// Empty Response, error object in case of error
func (c *client) Call(o Operation) (Response, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// send request header
	if err := c.begin(int32(2+8+o.Prefix.Len()+o.Data.Len()), o.Code, o.UID); err != nil {
		return Response{}, errors.Wrapf(err, "failed to send request header")
	}
	if o.Prefix.Len() > 0 {
		// send request prefix of body
		if err := c.write(o.Prefix.Bytes()); err != nil {
			return Response{}, errors.Wrapf(err, "failed to send request prefix of body")
		}
	}
	if o.Data.Len() > 0 {
		// send request body
		if err := c.write(o.Data.Bytes()); err != nil {
			return Response{}, errors.Wrapf(err, "failed to send request body")
		}
	}

	// receive server response
	r, err := c.commit()
	if err != nil {
		return r, errors.Wrapf(err, "failed to receive response from server")
	}
	if r.UID != o.UID {
		return r, errors.Errorf("invalid response id (expected %d, but received %d)", o.UID, r.UID)
	}

	return r, nil
}

// begin starts request by writing data directly to connection with server.
// Arguments:
// length - length in bytes of request message.
// code - code of operation.
// uid - request ID.
// Returns:
// nil in case of success.
// error object in case of error.
func (c *client) begin(length int32, code int16, uid int64) error {
	return writePrimitives(c.conn, length, code, uid)
}

// write writes primitives directly to connection with server.
// Arguments:
// primitives - primitives to write.
// Returns:
// nil in case of success.
// error object in case of error.
func (c *client) write(primitives ...interface{}) error {
	return writePrimitives(c.conn, primitives...)
}

// commit finishes the request and returns response from server.
// Returns:
// Response, nil in case of success.
// Empty Response, error object in case of error.
func (c *client) commit() (Response, error) {
	var r Response

	// read response message length
	if err := readPrimitives(c.conn, &r.Len); err != nil {
		return r, errors.Wrapf(err, "failed to read response message length")
	}

	// read response message
	b := make([]byte, r.Len, r.Len)
	if err := readPrimitives(c.conn, &b); err != nil {
		return r, errors.Wrapf(err, "failed to read response message")
	}
	r.Data = bytes.NewReader(b)

	// read response header
	if err := r.ReadPrimitives(&r.UID, &r.Status); err != nil {
		return r, errors.Wrapf(err, "failed to read response header")
	}

	if r.Status != errors.StatusSuccess {
		// Response status
		if err := r.ReadPrimitives(&r.Message); err != nil {
			return r, errors.Wrapf(err, "failed to read error message")
		}
	}
	return r, nil
}

// NewClient connects to the Apache Ignite cluster
func NewClient(ctx context.Context, network, address string, major, minor, patch int16) (Client, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open connection")
	}
	if err = handshake(conn, major, minor, patch); err != nil {
		conn.Close()
		return nil, errors.Wrapf(err, "failed to make handshake")
	}
	c := &client{conn: conn, debugID: strings.Join([]string{"network=", network, "', address='", address, "'"}, "")}
	runtime.SetFinalizer(c, clientFinalizer)
	return c, nil
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
		return errors.Wrapf(err, "failed to send handshake request")
	}

	// Receive handshake response
	var length int32
	var res byte
	if err := readPrimitives(rw, &length, &res); err != nil {
		return errors.Wrapf(err, "failed to read handshake response (length and result)")
	}
	if res != 1 {
		var msg string
		if err := readPrimitives(rw, &major, &minor, &patch, &msg); err != nil {
			return errors.Wrapf(err, "failed to read handshake response (supported protocol version and error message)")
		}
		return errors.Errorf("handshake failed, code=%d, message='%s', supported protocol version is v%d.%d.%d",
			res, msg, major, minor, patch)
	}

	return nil
}

// clientFinalizer is memory leak spy
func clientFinalizer(c *client) {
	if c.IsConnected() {
		debug.ResourceLeakLogger.Printf("client \"%s\" is not closed", c.debugID)
		c.Close()
	}
}
