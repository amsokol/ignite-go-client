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
