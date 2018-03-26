package ignite

import (
	"fmt"
	"math/rand"
)

// CachePut puts a value with a given key to cache (overwriting existing value if any).
func (c *client) CachePut(cache string, binary bool, key interface{}, value interface{}, status *int32) error {
	uid := rand.Int63()

	o := c.Prepare(opCachePut, uid)
	// prepare data
	if err := o.WritePrimitives(hashCode(cache), binary); err != nil {
		return fmt.Errorf("failed to write cache id and binary flag: %s", err.Error())
	}
	if err := o.WriteObjects(key, value); err != nil {
		return fmt.Errorf("failed to write cache key and value: %s", err.Error())
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
