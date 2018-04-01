package ignite

import (
	"fmt"
	"math/rand"
)

// CacheClearKey clears the cache key without notifying listeners or cache writers.
func (c *client) CacheClearKey(cache string, binary bool, key interface{}, status *int32) error {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	o := c.Prepare(opCacheClearKey, uid)
	// prepare data
	if err := o.WritePrimitives(hashCode(cache), binary); err != nil {
		return fmt.Errorf("failed to write cache id and binary flag: %s", err.Error())
	}
	if err := o.WriteObjects(key); err != nil {
		return fmt.Errorf("failed to write cache key: %s", err.Error())
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
