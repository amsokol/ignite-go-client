package ignite

import (
	"fmt"
	"math/rand"
)

// CacheGetSize gets the number of entries in cache.
func (c *client) CacheGetSize(cache string, binary bool, count int, modes []byte, status *int32) (int64, error) {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	o := c.Prepare(opCacheGetSize, uid)
	// prepare data
	if err := o.WritePrimitives(hashCode(cache), binary, int32(count)); err != nil {
		return 0, fmt.Errorf("failed to write cache id, binary flag and mode count: %s", err.Error())
	}
	if modes != nil || len(modes) > 0 {
		for i, m := range modes {
			if err := o.WritePrimitives(m); err != nil {
				return 0, fmt.Errorf("failed to write mode with index %d: %s", i, err.Error())
			}
		}
	} else {
		if err := o.WritePrimitives(byte(0)); err != nil {
			return 0, fmt.Errorf("failed to write mode ALL: %s", err.Error())
		}
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return 0, fmt.Errorf("failed to execute operation: %s", err.Error())
	}
	if r.UID != uid {
		return 0, fmt.Errorf("invalid response id (expected %d, but received %d)", uid, r.UID)
	}
	if status != nil {
		*status = r.Status
	}
	if r.Status != StatusSuccess {
		return 0, fmt.Errorf("failed to execute operation: status=%d, message=%s", r.Status, r.Message)
	}

	// read response data
	var size int64
	if err := r.ReadPrimitives(&size); err != nil {
		return 0, fmt.Errorf("failed to read result value: %s", err.Error())
	}

	return size, nil
}
