package ignite

import (
	"fmt"
	"math/rand"
)

// CacheCreateWithName Creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
func (c *client) CacheCreateWithName(cache string, status *int32) error {
	return c.cacheCreateWithName(opCacheCreateWithName, cache, status)
}

func (c *client) cacheCreateWithName(code int16, cache string, status *int32) error {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	r, err := c.Exec(code, uid, cache)
	if err != nil {
		return fmt.Errorf("failed to execute operation: %s", err.Error())
	}
	if r.UID != uid {
		return fmt.Errorf("invalid response id (expected %d, but received %d)", uid, r.UID)
	}
	if status != nil {
		*status = r.Status
	}
	if r.Status != StatusSuccess {
		return fmt.Errorf("failed to execute operation: status=%d, message=%s", r.Status, r.Message)
	}

	return nil
}
