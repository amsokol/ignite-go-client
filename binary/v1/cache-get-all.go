package ignite

import (
	"fmt"
	"math/rand"
)

// CacheGetAll retrieves multiple key-value pairs from cache.
func (c *client) CacheGetAll(cache string, binary bool, keys []interface{}, status *int32) (map[interface{}]interface{}, error) {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	o := c.Prepare(opCacheGetAll, uid)
	// prepare data
	if err := o.WritePrimitives(hashCode(cache), binary, int32(len(keys))); err != nil {
		return nil, fmt.Errorf("failed to write cache id, binary flag and key count: %s", err.Error())
	}
	for i, k := range keys {
		if err := o.WriteObjects(k); err != nil {
			return nil, fmt.Errorf("failed to write cache key with index %d: %s", i, err.Error())
		}
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
	var count int32
	if err := r.ReadPrimitives(&count); err != nil {
		return nil, fmt.Errorf("failed to read pairs count: %s", err.Error())
	}
	data := map[interface{}]interface{}{}
	for i := 0; i < int(count); i++ {
		pair, err := r.ReadObjects(2)
		if err != nil {
			return nil, fmt.Errorf("failed to read pair with index %d: %s", i, err.Error())
		}
		data[pair[0]] = pair[1]
	}

	return data, nil
}
