package ignite

import (
	"fmt"
	"math/rand"
)

// CacheClearKeys clears the cache keys without notifying listeners or cache writers.
func (c *client) CacheClearKeys(cache string, binary bool, keys []interface{}, status *int32) error {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	o := c.Prepare(opCacheClearKeys, uid)
	// prepare data
	if err := o.WritePrimitives(hashCode(cache), binary, int32(len(keys))); err != nil {
		return fmt.Errorf("failed to write cache id, binary flag and key count: %s", err.Error())
	}
	for i, k := range keys {
		if err := o.WriteObjects(k); err != nil {
			return fmt.Errorf("failed to write cache key with index %d: %s", i, err.Error())
		}
	}

	// execute
	r, err := c.Call(o)
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
