package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

const (
	// PeekModeAll is ALL
	PeekModeAll = 0
	// PeekModeNear is NEAR
	PeekModeNear = 1
	// PeekModePrimary is PRIMARY
	PeekModePrimary = 2
	// PeekModeBackup is BACKUP
	PeekModeBackup = 3
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

	return res.ReadObject()
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
	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	return res.ReadObject()
}

// CacheGetAndReplace puts a value with a given key to cache, returning previous value for that key,
// if and only if there is a value currently mapped for that key.
func (c *client) CacheGetAndReplace(cache string, binary bool, key interface{}, value interface{}) (interface{}, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetAndReplace)
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
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_AND_REPLACE operation")
	}
	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	return res.ReadObject()
}

// CacheGetAndRemove removes the cache entry with specified key, returning the value.
func (c *client) CacheGetAndRemove(cache string, binary bool, key interface{}) (interface{}, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetAndRemove)
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
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_AND_REMOVE operation")
	}
	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	return res.ReadObject()
}

// CachePutIfAbsent puts a value with a given key to cache only if the key does not already exist.
func (c *client) CachePutIfAbsent(cache string, binary bool, key interface{}, value interface{}) (bool, error) {
	// request and response
	req := NewRequestOperation(OpCachePutIfAbsent)
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
	if err := req.WriteObject(value); err != nil {
		return false, errors.Wrapf(err, "failed to write cache value")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_PUT_IF_ABSENT operation")
	}
	if err := res.CheckStatus(); err != nil {
		return false, err
	}

	return res.ReadBool()
}

// CacheGetAndPutIfAbsent puts a value with a given key to cache only if the key does not already exist.
func (c *client) CacheGetAndPutIfAbsent(cache string, binary bool, key interface{}, value interface{}) (interface{}, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetAndPutIfAbsent)
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
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_AND_PUT_IF_ABSENT operation")
	}
	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	return res.ReadObject()
}

// CacheReplace puts a value with a given key to cache only if the key already exists.
func (c *client) CacheReplace(cache string, binary bool, key interface{}, value interface{}) (bool, error) {
	// request and response
	req := NewRequestOperation(OpCacheReplace)
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
	if err := req.WriteObject(value); err != nil {
		return false, errors.Wrapf(err, "failed to write cache value")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_REPLACE operation")
	}
	if err := res.CheckStatus(); err != nil {
		return false, err
	}

	return res.ReadBool()
}

// CacheReplaceIfEquals puts a value with a given key to cache only if
// the key already exists and value equals provided value.
func (c *client) CacheReplaceIfEquals(cache string, binary bool, key interface{}, valueCompare interface{}, valueNew interface{}) (bool, error) {
	// request and response
	req := NewRequestOperation(OpCacheReplaceIfEquals)
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
	if err := req.WriteObject(valueCompare); err != nil {
		return false, errors.Wrapf(err, "failed to write cache value to compare")
	}
	if err := req.WriteObject(valueNew); err != nil {
		return false, errors.Wrapf(err, "failed to write new cache value")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_REPLACE_IF_EQUALS operation")
	}
	if err := res.CheckStatus(); err != nil {
		return false, err
	}

	return res.ReadBool()
}

// CacheClear clears the cache without notifying listeners or cache writers.
func (c *client) CacheClear(cache string, binary bool) error {
	// request and response
	req := NewRequestOperation(OpCacheClear)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return errors.Wrapf(err, "failed to write binary flag")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CLEAR operation")
	}
	return res.CheckStatus()
}

// CacheClearKey clears the cache key without notifying listeners or cache writers.
func (c *client) CacheClearKey(cache string, binary bool, key interface{}) error {
	// request and response
	req := NewRequestOperation(OpCacheClearKey)
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

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CLEAR_KEY operation")
	}
	return res.CheckStatus()
}

// CacheClearKeys clears the cache keys without notifying listeners or cache writers.
func (c *client) CacheClearKeys(cache string, binary bool, keys []interface{}) error {
	// request and response
	req := NewRequestOperation(OpCacheClearKeys)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return errors.Wrapf(err, "failed to write binary flag")
	}
	if err := req.WriteInt(int32(len(keys))); err != nil {
		return errors.Wrapf(err, "failed to write key count")
	}
	for i, k := range keys {
		if err := req.WriteObject(k); err != nil {
			return errors.Wrapf(err, "failed to write cache key with index %d", i)
		}
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CLEAR_KEYS operation")
	}

	return res.CheckStatus()
}

// CacheRemoveKey removes an entry with a given key, notifying listeners and cache writers.
func (c *client) CacheRemoveKey(cache string, binary bool, key interface{}) (bool, error) {
	// request and response
	req := NewRequestOperation(OpCacheRemoveKey)
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
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_REMOVE_KEY operation")
	}
	if err := res.CheckStatus(); err != nil {
		return false, err
	}

	return res.ReadBool()
}

// CacheRemoveIfEquals removes an entry with a given key if provided value is equal to actual value,
// notifying listeners and cache writers.
func (c *client) CacheRemoveIfEquals(cache string, binary bool, key interface{}, value interface{}) (bool, error) {
	// request and response
	req := NewRequestOperation(OpCacheRemoveIfEquals)
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
	if err := req.WriteObject(value); err != nil {
		return false, errors.Wrapf(err, "failed to write cache value")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_REMOVE_IF_EQUALS operation")
	}
	if err := res.CheckStatus(); err != nil {
		return false, err
	}

	return res.ReadBool()
}

// CacheGetSize gets the number of entries in cache.
func (c *client) CacheGetSize(cache string, binary bool, modes []byte) (int64, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetSize)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return 0, errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return 0, errors.Wrapf(err, "failed to write binary flag")
	}
	var count int32
	if modes != nil || len(modes) > 0 {
		count = int32(len(modes))
	}
	if err := req.WriteInt(count); err != nil {
		return 0, errors.Wrapf(err, "failed to write binary flag")
	}
	if count > 0 {
		for i, m := range modes {
			if err := req.WriteByte(m); err != nil {
				return 0, errors.Wrapf(err, "failed to write mode with index %d", i)
			}
		}
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return 0, errors.Wrapf(err, "failed to execute OP_CACHE_GET_SIZE operation")
	}
	if err := res.CheckStatus(); err != nil {
		return 0, err
	}

	return res.ReadLong()
}
