package ignite

import (
	"fmt"
	"math/rand"
)

// QuerySQLData input parameter for QuerySQL func
type QuerySQLData struct {
	// Name of a type or SQL table.
	Table string

	// SQL query string.
	Query string

	// Query arguments.
	QueryArgs []interface{}

	// Distributed joins.
	DistributedJoins bool

	// Local query.
	LocalQuery bool

	// Replicated only - Whether query contains only replicated tables or not.
	ReplicatedOnly bool

	// Cursor page size.
	PageSize int

	// Timeout(milliseconds) value should be non-negative. Zero value disables timeout.
	Timeout int64
}

// QuerySQLPage is query result page
type QuerySQLPage struct {
	// Keys (field names)
	Keys []interface{}

	// Values
	Values []interface{}

	// Indicates whether more results are available to be fetched with QuerySQLCursorGetPage.
	// When true, query cursor is closed automatically.
	HasMore bool
}

// QuerySQLResult output from QuerySQL func
type QuerySQLResult struct {
	// Cursor id. Can be closed with ResourceClose func.
	ID int64

	// Query result first page
	QuerySQLPage
}

func (c *client) QuerySQL(cache string, binary bool, data QuerySQLData, status *int32) (QuerySQLResult, error) {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()
	o := c.Prepare(opQuerySQL, uid)

	var res QuerySQLResult

	if err := o.WritePrimitives(hashCode(cache), binary, data.Table, data.Query); err != nil {
		return res, fmt.Errorf("failed to write request data: %s", err.Error())
	}

	var l int32
	if data.QueryArgs != nil {
		l = int32(len(data.QueryArgs))
	}
	// write args
	if err := o.WritePrimitives(l); err != nil {
		return res, fmt.Errorf("failed to write query arg count: %s", err.Error())
	}
	if l > 0 {
		for i, v := range data.QueryArgs {
			if err := o.WriteObjects(v); err != nil {
				return res, fmt.Errorf("failed to write query arg with index %d: %s", i, err.Error())
			}
		}
	}

	if err := o.WritePrimitives(data.DistributedJoins, data.LocalQuery, data.ReplicatedOnly,
		int32(data.PageSize), data.Timeout); err != nil {
		return res, fmt.Errorf("failed to write request data: %s", err.Error())
	}

	// execute
	r, err := c.Call(o)
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

	var count int32
	if err = r.ReadPrimitives(&res.ID, &count); err != nil {
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
