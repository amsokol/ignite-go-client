package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

// Key-Value Queries
// See for details:
// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations

// CacheGet retrieves a value from cache by key.
func (c *client) CacheGet(cache string, binary bool, key interface{}) (interface{}, error) {
	// request and response
	req := NewRequestOperation(OpCacheGet)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write binary flag")
	}
	if err := req.WriteObject(key); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache key")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET operation")
	}
	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	// read response data
	o, err := res.ReadObject()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read value object")
	}

	return o, nil
}

// CacheGetAll retrieves multiple key-value pairs from cache.
func (c *client) CacheGetAll(cache string, binary bool, keys []interface{}) (map[interface{}]interface{}, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetAll)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write binary flag")
	}
	if err := req.WriteInt(int32(len(keys))); err != nil {
		return nil, errors.Wrapf(err, "failed to write key count")
	}
	for i, k := range keys {
		if err := req.WriteObject(k); err != nil {
			return nil, errors.Wrapf(err, "failed to write cache key with index %d", i)
		}
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_ALL operation")
	}
	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	// read response data
	count, err := res.ReadInt()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read pairs count")
	}
	data := map[interface{}]interface{}{}
	for i := 0; i < int(count); i++ {
		key, err := res.ReadObject()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read key with index %d", i)
		}
		value, err := res.ReadObject()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read value with index %d", i)
		}
		data[key] = value
	}

	return data, nil
}

// CachePut puts a value with a given key to cache (overwriting existing value if any).
func (c *client) CachePut(cache string, binary bool, key interface{}, value interface{}) error {
	// request and response
	req := NewRequestOperation(OpCachePut)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return errors.Wrapf(err, "failed to write binary flag")
	}
	if err := req.WriteObject(key); err != nil {
		return errors.Wrapf(err, "failed to write cache key")
	}
	if err := req.WriteObject(value); err != nil {
		return errors.Wrapf(err, "failed to write cache value")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_PUT operation")
	}

	return res.CheckStatus()
}

// CachePutAll puts a value with a given key to cache (overwriting existing value if any).
func (c *client) CachePutAll(cache string, binary bool, data map[interface{}]interface{}) error {
	// request and response
	req := NewRequestOperation(OpCachePutAll)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return errors.Wrapf(err, "failed to write binary flag")
	}
	if err := req.WriteInt(int32(len(data))); err != nil {
		return errors.Wrapf(err, "failed to write key count")
	}
	for k, v := range data {
		if err := req.WriteObject(k); err != nil {
			return errors.Wrapf(err, "failed to write cache key")
		}
		if err := req.WriteObject(v); err != nil {
			return errors.Wrapf(err, "failed to write cache value")
		}
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_PUT_ALL operation")
	}

	return res.CheckStatus()
}

// CacheContainsKey returns a value indicating whether given key is present in cache.
func (c *client) CacheContainsKey(cache string, binary bool, key interface{}) (bool, error) {
	// request and response
	req := NewRequestOperation(OpCacheContainsKey)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return false, errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return false, errors.Wrapf(err, "failed to write binary flag")
	}
	if err := req.WriteObject(key); err != nil {
		return false, errors.Wrapf(err, "failed to write cache key")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_CONTAINS_KEY operation")
	}
	if err := res.CheckStatus(); err != nil {
		return false, err
	}

	return res.ReadBool()
}

// CacheContainsKeys returns a value indicating whether all given keys are present in cache.
func (c *client) CacheContainsKeys(cache string, binary bool, keys []interface{}) (bool, error) {
	// request and response
	req := NewRequestOperation(OpCacheContainsKeys)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return false, errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return false, errors.Wrapf(err, "failed to write binary flag")
	}
	if err := req.WriteInt(int32(len(keys))); err != nil {
		return false, errors.Wrapf(err, "failed to write key count")
	}
	for i, k := range keys {
		if err := req.WriteObject(k); err != nil {
			return false, errors.Wrapf(err, "failed to write cache key with index %d", i)
		}
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_CONTAINS_KEYS operation")
	}
	if err := res.CheckStatus(); err != nil {
		return false, err
	}

	return res.ReadBool()
}

// CacheGetAndPut puts a value with a given key to cache, and returns the previous value for that key.
func (c *client) CacheGetAndPut(cache string, binary bool, key interface{}, value interface{}) (interface{}, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetAndPut)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write binary flag")
	}
	if err := req.WriteObject(key); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache key")
	}
	if err := req.WriteObject(value); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache value")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_AND_PUT operation")
	}

	// read response data
	o, err := res.ReadObject()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read value object")
	}

	return o, nil
}
