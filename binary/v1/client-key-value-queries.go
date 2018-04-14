package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

// Key-Value Queries
// See for details:
// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations

// CacheGet retrieves a value from cache by key.
func (c *client) CacheGet(cache string, binary bool, key interface{}) (interface{}, error) {
	o := c.Prepare(OpCacheGet)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache key and value")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET operation")
	}
	if err = r.CheckStatus(); err != nil {
		return nil, err
	}

	// read response data
	object, err := r.ReadObject()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read value object")
	}

	return object, nil
}

// CacheGetAll retrieves multiple key-value pairs from cache.
func (c *client) CacheGetAll(cache string, binary bool, keys []interface{}) (map[interface{}]interface{}, error) {
	o := c.Prepare(OpCacheGetAll)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary, int32(len(keys))); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache id, binary flag and key count")
	}
	for i, k := range keys {
		if err := o.WriteObjects(k); err != nil {
			return nil, errors.Wrapf(err, "failed to write cache key with index %d", i)
		}
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_ALL operation")
	}
	if err = r.CheckStatus(); err != nil {
		return nil, err
	}

	// read response data
	var count int32
	if err := r.ReadPrimitives(&count); err != nil {
		return nil, errors.Wrapf(err, "failed to read pairs count")
	}
	data := map[interface{}]interface{}{}
	for i := 0; i < int(count); i++ {
		pair, err := r.ReadObjects(2)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read pair with index %d", i)
		}
		data[pair[0]] = pair[1]
	}

	return data, nil
}

// CachePut puts a value with a given key to cache (overwriting existing value if any).
func (c *client) CachePut(cache string, binary bool, key interface{}, value interface{}) error {
	o := c.Prepare(OpCachePut)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key, value); err != nil {
		return errors.Wrapf(err, "failed to write cache key and value")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_PUT operation")
	}

	return r.CheckStatus()
}

// CachePutAll puts a value with a given key to cache (overwriting existing value if any).
func (c *client) CachePutAll(cache string, binary bool, data map[interface{}]interface{}) error {
	o := c.Prepare(OpCachePutAll)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary, int32(len(data))); err != nil {
		return errors.Wrapf(err, "failed to write cache id, binary flag and pairs count")
	}
	for k, v := range data {
		if err := o.WriteObjects(k, v); err != nil {
			return errors.Wrapf(err, "failed to write cache key and value")
		}
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_PUT_ALL operation")
	}

	return r.CheckStatus()
}

// CacheContainsKey returns a value indicating whether given key is present in cache.
func (c *client) CacheContainsKey(cache string, binary bool, key interface{}) (bool, error) {
	o := c.Prepare(OpCacheContainsKey)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return false, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key); err != nil {
		return false, errors.Wrapf(err, "failed to write cache key")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_CONTAINS_KEY operation")
	}
	if err = r.CheckStatus(); err != nil {
		return false, err
	}

	// read response data
	var res bool
	if err = r.ReadPrimitives(&res); err != nil {
		return false, errors.Wrapf(err, "failed to read result value")
	}

	return res, nil
}

// CacheContainsKeys returns a value indicating whether all given keys are present in cache.
func (c *client) CacheContainsKeys(cache string, binary bool, keys []interface{}) (bool, error) {
	o := c.Prepare(OpCacheContainsKeys)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary, int32(len(keys))); err != nil {
		return false, errors.Wrapf(err, "failed to write cache id, binary flag and key count")
	}
	for i, k := range keys {
		if err := o.WriteObjects(k); err != nil {
			return false, errors.Wrapf(err, "failed to write cache key with index %d", i)
		}
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_CONTAINS_KEYS operation")
	}
	if err = r.CheckStatus(); err != nil {
		return false, err
	}

	// read response data
	var res bool
	if err := r.ReadPrimitives(&res); err != nil {
		return false, errors.Wrapf(err, "failed to read result value")
	}

	return res, nil
}

// CacheGetAndPut puts a value with a given key to cache, and returns the previous value for that key.
func (c *client) CacheGetAndPut(cache string, binary bool, key interface{}, value interface{}) (interface{}, error) {
	o := c.Prepare(OpCacheGetAndPut)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key, value); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache key and value")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_AND_PUT operation")
	}
	if err = r.CheckStatus(); err != nil {
		return nil, err
	}

	// read response data
	object, err := r.ReadObject()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read value object")
	}

	return object, nil
}

// CacheGetAndReplace puts a value with a given key to cache, returning previous value for that key,
// if and only if there is a value currently mapped for that key.
func (c *client) CacheGetAndReplace(cache string, binary bool, key interface{}, value interface{}) (interface{}, error) {
	o := c.Prepare(OpCacheGetAndReplace)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key, value); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache key and value")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute operation")
	}
	if err = r.CheckStatus(); err != nil {
		return nil, err
	}

	// read response data
	object, err := r.ReadObject()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read value object")
	}

	return object, nil
}

// CacheGetAndRemove removes the cache entry with specified key, returning the value.
func (c *client) CacheGetAndRemove(cache string, binary bool, key interface{}) (interface{}, error) {
	o := c.Prepare(OpCacheGetAndRemove)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache key and value")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_AND_REMOVE operation")
	}
	if err = r.CheckStatus(); err != nil {
		return nil, err
	}

	// read response data
	object, err := r.ReadObject()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read value object")
	}

	return object, nil
}

// CachePutIfAbsent puts a value with a given key to cache only if the key does not already exist.
func (c *client) CachePutIfAbsent(cache string, binary bool, key interface{}, value interface{}) (bool, error) {
	o := c.Prepare(OpCachePutIfAbsent)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return false, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key, value); err != nil {
		return false, errors.Wrapf(err, "failed to write cache key and value")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_PUT_IF_ABSENT operation")
	}
	if err = r.CheckStatus(); err != nil {
		return false, err
	}

	// read response data
	var res bool
	if err := r.ReadPrimitives(&res); err != nil {
		return false, errors.Wrapf(err, "failed to read result value")
	}

	return res, nil
}

// CacheGetAndPutIfAbsent puts a value with a given key to cache only if the key does not already exist.
func (c *client) CacheGetAndPutIfAbsent(cache string, binary bool, key interface{}, value interface{}) (interface{}, error) {
	o := c.Prepare(OpCacheGetAndPutIfAbsent)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key, value); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache key and value")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_AND_PUT_IF_ABSENT operation")
	}
	if err = r.CheckStatus(); err != nil {
		return nil, err
	}

	// read response data
	object, err := r.ReadObject()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read value object")
	}

	return object, nil
}

// CacheReplace puts a value with a given key to cache only if the key already exists.
func (c *client) CacheReplace(cache string, binary bool, key interface{}, value interface{}) (bool, error) {
	o := c.Prepare(OpCacheReplace)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return false, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key, value); err != nil {
		return false, errors.Wrapf(err, "failed to write cache key and value")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_REPLACE operation")
	}
	if err = r.CheckStatus(); err != nil {
		return false, err
	}

	// read response data
	var res bool
	if err := r.ReadPrimitives(&res); err != nil {
		return false, errors.Wrapf(err, "failed to read result value")
	}

	return res, nil
}

// CacheReplaceIfEquals puts a value with a given key to cache only if
// the key already exists and value equals provided value.
func (c *client) CacheReplaceIfEquals(cache string, binary bool, key interface{}, valueCompare interface{}, valueNew interface{}) (bool, error) {
	o := c.Prepare(OpCacheReplaceIfEquals)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return false, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key, valueCompare, valueNew); err != nil {
		return false, errors.Wrapf(err, "failed to write cache key and values")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_REPLACE_IF_EQUALS operation")
	}
	if err = r.CheckStatus(); err != nil {
		return false, err
	}

	// read response data
	var res bool
	if err := r.ReadPrimitives(&res); err != nil {
		return false, errors.Wrapf(err, "failed to read result value")
	}

	return res, nil
}

// CacheClear clears the cache without notifying listeners or cache writers.
func (c *client) CacheClear(cache string, binary bool) error {
	r, err := c.Exec(OpCacheClear, HashCode(cache), binary)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CLEAR operation")
	}
	return r.CheckStatus()
}

// CacheClearKey clears the cache key without notifying listeners or cache writers.
func (c *client) CacheClearKey(cache string, binary bool, key interface{}) error {
	o := c.Prepare(OpCacheClearKey)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key); err != nil {
		return errors.Wrapf(err, "failed to write cache key")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CLEAR_KEY operation")
	}
	return r.CheckStatus()
}

// CacheClearKeys clears the cache keys without notifying listeners or cache writers.
func (c *client) CacheClearKeys(cache string, binary bool, keys []interface{}) error {
	o := c.Prepare(OpCacheClearKeys)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary, int32(len(keys))); err != nil {
		return errors.Wrapf(err, "failed to write cache id, binary flag and key count")
	}
	for i, k := range keys {
		if err := o.WriteObjects(k); err != nil {
			return errors.Wrapf(err, "failed to write cache key with index %d", i)
		}
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CLEAR_KEYS operation")
	}

	return r.CheckStatus()
}

// CacheRemoveKey removes an entry with a given key, notifying listeners and cache writers.
func (c *client) CacheRemoveKey(cache string, binary bool, key interface{}) (bool, error) {
	o := c.Prepare(OpCacheRemoveKey)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return false, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key); err != nil {
		return false, errors.Wrapf(err, "failed to write cache key")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_REMOVE_KEY operation")
	}
	if err = r.CheckStatus(); err != nil {
		return false, err
	}

	// read response data
	var res bool
	if err := r.ReadPrimitives(&res); err != nil {
		return false, errors.Wrapf(err, "failed to read result value")
	}

	return res, nil
}

// CacheRemoveIfEquals removes an entry with a given key if provided value is equal to actual value,
// notifying listeners and cache writers.
func (c *client) CacheRemoveIfEquals(cache string, binary bool, key interface{}, value interface{}) (bool, error) {
	o := c.Prepare(OpCacheRemoveIfEquals)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return false, errors.Wrapf(err, "failed to write cache id and binary flag")
	}
	if err := o.WriteObjects(key, value); err != nil {
		return false, errors.Wrapf(err, "failed to write cache key")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return false, errors.Wrapf(err, "failed to execute OP_CACHE_REMOVE_IF_EQUALS operation")
	}
	if err = r.CheckStatus(); err != nil {
		return false, err
	}

	// read response data
	var res bool
	if err := r.ReadPrimitives(&res); err != nil {
		return false, errors.Wrapf(err, "failed to read result value")
	}

	return res, nil
}

// CacheGetSize gets the number of entries in cache.
func (c *client) CacheGetSize(cache string, binary bool, count int, modes []byte) (int64, error) {
	o := c.Prepare(OpCacheGetSize)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary, int32(count)); err != nil {
		return 0, errors.Wrapf(err, "failed to write cache id, binary flag and mode count")
	}
	if modes != nil || len(modes) > 0 {
		for i, m := range modes {
			if err := o.WritePrimitives(m); err != nil {
				return 0, errors.Wrapf(err, "failed to write mode with index %d", i)
			}
		}
	} else {
		if err := o.WritePrimitives(byte(0)); err != nil {
			return 0, errors.Wrapf(err, "failed to write mode ALL")
		}
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to execute OP_CACHE_GET_SIZE operation")
	}
	if err = r.CheckStatus(); err != nil {
		return 0, err
	}

	// read response data
	var size int64
	if err := r.ReadPrimitives(&size); err != nil {
		return 0, errors.Wrapf(err, "failed to read result value")
	}

	return size, nil
}

// CacheRemoveKeys removes entries with given keys, notifying listeners and cache writers.
func (c *client) CacheRemoveKeys(cache string, binary bool, keys []interface{}) error {
	o := c.Prepare(OpCacheRemoveKeys)
	// prepare data
	if err := o.WritePrimitives(HashCode(cache), binary, int32(len(keys))); err != nil {
		return errors.Wrapf(err, "failed to write cache id, binary flag and key count")
	}
	for i, k := range keys {
		if err := o.WriteObjects(k); err != nil {
			return errors.Wrapf(err, "failed to write cache key with index %d", i)
		}
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_REMOVE_KEYS operation")
	}

	return r.CheckStatus()
}

// CacheRemoveAll destroys cache with a given name.
func (c *client) CacheRemoveAll(cache string, binary bool) error {
	r, err := c.Exec(OpCacheRemoveAll, HashCode(cache), binary)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_REMOVE_ALL operation")
	}

	return r.CheckStatus()
}
