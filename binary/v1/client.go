package ignite

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/amsokol/ignite-go-client/binary/errors"
	"github.com/amsokol/ignite-go-client/debug"
)

// Client is interface to communicate with Apache Ignite cluster.
// Client is thread safe.
type Client interface {
	// Connected return true if connection to the cluster is active
	Connected() bool

	// Do sends request and receives response
	Do(req Request, res Response) error

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

	// CacheGet retrieves a value from cache by key.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get
	CacheGet(cache string, binary bool, key interface{}) (interface{}, error)

	// CacheGetAll retrieves multiple key-value pairs from cache.
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_get_all
	CacheGetAll(cache string, binary bool, keys []interface{}) (map[interface{}]interface{}, error)

	// CachePut puts a value with a given key to cache (overwriting existing value if any).
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_put
	CachePut(cache string, binary bool, key interface{}, value interface{}) error

	// CachePutAll puts a value with a given key to cache (overwriting existing value if any).
	// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#section-op_cache_put_all
	CachePutAll(cache string, binary bool, data map[interface{}]interface{}) error

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
	CacheGetSize(cache string, binary bool, modes []byte) (int64, error)
}

type client struct {
	debugID string
	conn    net.Conn
	mutex   *sync.Mutex

	Client
}

// IsConnected return true if connection to the cluster is active
func (c *client) Connected() bool {
	return c.conn != nil
}

// Do sends request and receives response
func (c *client) Do(req Request, res Response) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// send request
	if _, err := req.WriteTo(c.conn); err != nil {
		return errors.Wrapf(err, "failed to send request to server")
	}

	// receive response
	_, err := res.ReadFrom(c.conn)

	return err
}

// Close closes connection.
// Returns:
// nil in case of success.
// error object in case of error.
func (c *client) Close() error {
	if c.Connected() {
		defer func() { c.conn = nil }()
		return c.conn.Close()
	}
	return nil
}

// Connect connects to the Apache Ignite cluster
// Returns: client
func Connect(ctx context.Context, network, host string, port, major, minor, patch int) (Client, error) {
	address := fmt.Sprintf("%s:%d", host, port)

	// connect
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open connection")
	}

	c := &client{conn: conn, debugID: strings.Join([]string{"network=", network, "', address='", address, "'"}, ""),
		mutex: &sync.Mutex{}}
	runtime.SetFinalizer(c, clientFinalizer)

	// request and response
	req := NewRequestHandshake(major, minor, patch)
	res := &ResponseHandshake{}

	// make handshake
	if err = c.Do(req, res); err != nil {
		c.Close()
		return nil, errors.Wrapf(err, "failed to make handshake")
	}

	if !res.Success {
		c.Close()
		return nil, errors.Errorf("handshake failed: %s, server supported protocol version is v%d.%d.%d",
			res.Message, res.Major, res.Minor, res.Patch)
	}

	// return connected client
	return c, nil
}

// clientFinalizer is resource leak spy
func clientFinalizer(c *client) {
	if c.Connected() {
		debug.ResourceLeakLogger.Printf("client \"%s\" is not closed", c.debugID)
		c.Close()
	}
}
