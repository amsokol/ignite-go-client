package ignite

import (
	"fmt"
	"math/rand"
)

// CacheContainsKeys returns a value indicating whether all given keys are present in cache.
func (c *client) CacheContainsKeys(cache string, binary bool, keys []interface{}, status *int32) (bool, error) {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	o := c.Prepare(opCacheContainsKeys, uid)
	// prepare data
	if err := o.WritePrimitives(hashCode(cache), binary, int32(len(keys))); err != nil {
		return false, fmt.Errorf("failed to write cache id, binary flag and key count: %s", err.Error())
	}
	for i, k := range keys {
		if err := o.WriteObjects(k); err != nil {
			return false, fmt.Errorf("failed to write cache key with index %d: %s", i, err.Error())
		}
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return false, fmt.Errorf("failed to execute operation: %s", err.Error())
	}
	if r.UID != uid {
		return false, fmt.Errorf("invalid response id (expected %d, but received %d)", uid, r.UID)
	}
	if status != nil {
		*status = r.Status
	}
	if r.Status != StatusSuccess {
		return false, fmt.Errorf("failed to execute operation: status=%d, message=%s", r.Status, r.Message)
	}

	// read response data
	var res bool
	if err := r.ReadPrimitives(&res); err != nil {
		return false, fmt.Errorf("failed to read result value: %s", err.Error())
	}

	return res, nil
}
