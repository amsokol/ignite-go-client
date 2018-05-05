package common

import (
	"github.com/amsokol/ignite-go-client/binary/v1"
)

// ConnInfo contains Apache Ignite cluster connection and query execution parameters
type ConnInfo struct {
	URL string

	// Apache Ignite client connection information
	ignite.ConnInfo

	Cache string

	// Schema for the query; can be empty, in which case default PUBLIC schema will be used.
	Schema string

	// Query cursor page size.
	PageSize int

	// Max rows.
	MaxRows int

	// Timeout(milliseconds) value should be non-negative. Zero value disables timeout.
	Timeout int64

	// Distributed joins.
	DistributedJoins bool

	// Local query.
	LocalQuery bool

	// Replicated only - Whether query contains only replicated tables or not.
	ReplicatedOnly bool

	// Enforce join order.
	EnforceJoinOrder bool

	// Collocated - Whether your data is co-located or not.
	Collocated bool

	// Lazy query execution.
	LazyQuery bool
}
