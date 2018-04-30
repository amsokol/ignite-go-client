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
