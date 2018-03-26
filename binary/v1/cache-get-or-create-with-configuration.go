package ignite

// CacheGetOrCreateWithConfiguration creates cache with provided configuration. Does nothing if the name is already in use.
func (c *client) CacheGetOrCreateWithConfiguration(cc *CacheConfigurationRefs, status *int32) error {
	return c.cacheCreateWithConfiguration(opCacheGetOrCreateWithConfiguration, cc, status)
}
