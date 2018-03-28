package ignite

import (
	"fmt"
	"math/rand"
)

// CachePutAll puts a value with a given key to cache (overwriting existing value if any).
func (c *client) CachePutAll(cache string, binary bool, data map[interface{}]interface{}, status *int32) error {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	o := c.Prepare(opCachePutAll, uid)
	// prepare data
	if err := o.WritePrimitives(hashCode(cache), binary, int32(len(data))); err != nil {
		return fmt.Errorf("failed to write cache id, binary flag and pairs count: %s", err.Error())
	}
	for k, v := range data {
		if err := o.WriteObjects(k, v); err != nil {
			return fmt.Errorf("failed to write cache key and value: %s", err.Error())
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
