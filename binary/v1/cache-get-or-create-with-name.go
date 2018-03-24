package ignite

import (
	"fmt"
	"math/rand"
)

// CacheGetOrCreateWithName creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
// Does nothing if the cache exists.
func (c *client) CacheGetOrCreateWithName(name string, status *int32) error {
	uid := rand.Int63()

	r, err := c.Exec(opCacheGetOrCreateWithName, uid, name)
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
