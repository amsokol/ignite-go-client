package ignite

// CacheGetOrCreateWithName creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
// Does nothing if the cache exists.
func (c *client) CacheGetOrCreateWithName(name string, status *int32) error {
	return c.cacheCreateWithName(opCacheGetOrCreateWithName, name, status)
}
