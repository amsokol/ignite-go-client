package ignite

import (
	"fmt"
	"math/rand"
)

// CacheDestroy destroys cache with a given name.
func (c *client) CacheDestroy(cache string, status *int32) error {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	r, err := c.Exec(opCacheDestroy, uid, hashCode(cache))
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
