package ignite

import (
	"fmt"
	"math/rand"
)

// CacheGet retrieves a value from cache by key.
func (c *client) CacheGet(cache string, binary bool, key interface{}, status *int32) (interface{}, error) {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	o := c.Prepare(opCacheGet, uid)
	// prepare data
	if err := o.WritePrimitives(hashCode(cache), binary); err != nil {
		return nil, fmt.Errorf("failed to write cache id and binary flag: %s", err.Error())
	}
	if err := o.WriteObjects(key); err != nil {
		return nil, fmt.Errorf("failed to write cache key and value: %s", err.Error())
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return nil, fmt.Errorf("failed to execute operation: %s", err.Error())
	}
	if r.UID != uid {
		return nil, fmt.Errorf("invalid response id (expected %d, but received %d)", uid, r.UID)
	}
	if status != nil {
		*status = r.Status
	}
	if r.Status != StatusSuccess {
		return nil, fmt.Errorf("failed to execute operation: status=%d, message=%s", r.Status, r.Message)
	}

	// read response data
	objects, err := r.ReadObjects(1)
	if err != nil {
		return nil, fmt.Errorf("failed to read value object: %s", err.Error())
	}

	return objects[0], nil
}
