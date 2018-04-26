package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

// CacheCreateWithName Creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_create_with_name
func (c *client) CacheCreateWithName(cache string) error {
	return errors.Errorf("not implemented")
}
