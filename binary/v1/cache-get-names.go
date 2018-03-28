package ignite

import (
	"fmt"
	"math/rand"
)

// CacheGetNames returns existing cache names.
func (c *client) CacheGetNames(status *int32) ([]string, error) {
	uid := rand.Int63()

	r, err := c.Exec(opCacheGetNames, uid)
	if err != nil {
		return []string{}, fmt.Errorf("failed to execute operation: %s", err.Error())
	}
	if r.UID != uid {
		return []string{}, fmt.Errorf("invalid response id (expected %d, but received %d)", uid, r.UID)
	}
	if status != nil {
		*status = r.Status
	}
	if r.Status != StatusSuccess {
		return []string{}, fmt.Errorf("failed to execute operation: status=%d, message=%s", r.Status, r.Message)
	}

	// get cache count
	var count int32
	if err := r.ReadPrimitives(&count); err != nil {
		return []string{}, fmt.Errorf("failed to read cache count: %s", err.Error())
	}

	// read cache names
	names := make([]string, 0, int(count))
	for i := 0; i < int(count); i++ {
		var name string
		if err := r.ReadPrimitives(&name); err != nil {
			return []string{}, fmt.Errorf("failed to read cache name with index %d, reason: %s", i, err.Error())
		}
		names = append(names, name)
	}

	return names, nil
}
