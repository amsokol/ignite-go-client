package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

// CacheCreateWithName Creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_create_with_name
func (c *client) CacheCreateWithName(cache string) error {
	// request and response
	req := NewRequestOperation(OpCacheCreateWithName)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteOString(cache); err != nil {
		return errors.Wrapf(err, "failed to write cache name")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CREATE_WITH_NAME operation")
	}

	return res.CheckStatus()
}

// CacheGetOrCreateWithName creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
// Does nothing if the cache exists.
func (c *client) CacheGetOrCreateWithName(cache string) error {
	// request and response
	req := NewRequestOperation(OpCacheGetOrCreateWithName)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteOString(cache); err != nil {
		return errors.Wrapf(err, "failed to write cache name")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_GET_OR_CREATE_WITH_NAME operation")
	}

	return res.CheckStatus()
}

// CacheGetNames returns existing cache names.
func (c *client) CacheGetNames() ([]string, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetNames)
	res := NewResponseOperation(req.UID)

	// execute operation
	if err := c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_NAMES operation")
	}

	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	// get cache count
	count, err := res.ReadInt()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read cache name count")
	}

	// read cache names
	names := make([]string, 0, int(count))
	for i := 0; i < int(count); i++ {
		name, _, err := res.ReadOString()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read cache name with index %d", i)
		}
		names = append(names, name)
	}

	return names, nil
}
