package ignite

import (
	"fmt"
	"math/rand"
)

// QuerySQLFieldsCursorGetPage retrieves the next query result page by cursor id from QuerySQLFields.
func (c *client) QuerySQLFieldsCursorGetPage(id int64, fieldCount int, status *int32) (QuerySQLFieldsPage, error) {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()
	var res QuerySQLFieldsPage

	r, err := c.Exec(opQuerySQLFieldsCursorGetPage, uid, id)
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
	var rowCount int32
	if err = r.ReadPrimitives(&rowCount); err != nil {
		return res, fmt.Errorf("failed to read row count: %s", err.Error())
	}
	res.Rows = make([][]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		res.Rows[i], err = r.ReadObjects(fieldCount)
		if err != nil {
			return res, fmt.Errorf("failed to read row with index %d: %s", i, err.Error())
		}
	}
	if err = r.ReadPrimitives(&res.HasMore); err != nil {
		return res, fmt.Errorf("failed to read response data: %s", err.Error())
	}

	return res, nil
}
