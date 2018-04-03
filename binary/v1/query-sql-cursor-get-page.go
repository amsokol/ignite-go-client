package ignite

import (
	"fmt"
	"math/rand"
)

// QuerySQLCursorGetPage retrieves the next SQL query cursor page by cursor id from QuerySQL.
func (c *client) QuerySQLCursorGetPage(id int64, status *int32) (QuerySQLPage, error) {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()
	var res QuerySQLPage

	r, err := c.Exec(opQuerySQLCursorGetPage, uid, id)
	if err != nil {
		return res, fmt.Errorf("failed to execute operation: %s", err.Error())
	}
	if r.UID != uid {
		return res, fmt.Errorf("invalid response id (expected %d, but received %d)", uid, r.UID)
	}
	if status != nil {
		*status = r.Status
	}
	if r.Status != StatusSuccess {
		return res, fmt.Errorf("failed to execute operation: status=%d, message=%s", r.Status, r.Message)
	}

	// read data
	var count int32
	if err = r.ReadPrimitives(&count); err != nil {
		return res, fmt.Errorf("failed to read response data: %s", err.Error())
	}
	res.Keys = make([]interface{}, 0, int(count))
	res.Values = make([]interface{}, 0, int(count))
	// read data
	for i := 0; i < int(count); i++ {
		pair, err := r.ReadObjects(2)
		if err != nil {
			return res, fmt.Errorf("failed to read pair with index %d: %s", i, err.Error())
		}
		res.Keys = append(res.Keys, pair[0])
		res.Values = append(res.Values, pair[1])
	}
	if err = r.ReadPrimitives(&res.HasMore); err != nil {
		return res, fmt.Errorf("failed to read response data: %s", err.Error())
	}

	return res, nil
}
