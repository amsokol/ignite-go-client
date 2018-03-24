package ignite

import (
	"fmt"
)

const (
	// CacheModeLocal is LOCAL = 0
	CacheModeLocal = 0
	// CacheModeReplicated is REPLICATED = 1
	CacheModeReplicated = 1
	// CacheModePartitioned is PARTITIONED  = 2
	CacheModePartitioned = 2

	// PartitionLossPolicyReadOnlySafe is READ_ONLY_SAFE = 0
	PartitionLossPolicyReadOnlySafe = 0
	// PartitionLossPolicyReadOnlyAll is READ_ONLY_ALL = 1
	PartitionLossPolicyReadOnlyAll = 1
	// PartitionLossPolicyReadWriteSafe is READ_WRITE_SAFE = 2
	PartitionLossPolicyReadWriteSafe = 2
	// PartitionLossPolicyReadWriteAll is READ_WRITE_ALL = 3
	PartitionLossPolicyReadWriteAll = 3
	// PartitionLossPolicyIgnore is IGNORE = 4
	PartitionLossPolicyIgnore = 4

	// RebalanceModeSync is SYNC = 0
	RebalanceModeSync = 0
	// RebalanceModeASync is ASYNC = 1
	RebalanceModeASync = 0
	// RebalanceModeNone is NONE = 2
	RebalanceModeNone = 2

	// WriteSynchronizationModeFullSync is FULL_SYNC = 0
	WriteSynchronizationModeFullSync = 0
	// WriteSynchronizationModeFullASync is FULL_ASYNC = 1
	WriteSynchronizationModeFullASync = 1
	// WriteSynchronizationModePrimarySync is PRIMARY_SYNC = 2
	WriteSynchronizationModePrimarySync = 2

	// QueryIndexTypeSorted is SORTED = 0
	QueryIndexTypeSorted = 0
	// QueryIndexTypeFullText is FULLTEXT = 1
	QueryIndexTypeFullText = 1
	// QueryIndexTypeGeospatial is GEOSPATIAL = 2
	QueryIndexTypeGeospatial = 2
)

// CacheConfiguration describes cache configuration
type CacheConfiguration struct {
	NumberOfBackups               int32
	CacheMode                     int32
	CopyOnRead                    bool
	DataRegionName                string
	EagerTTL                      bool
	StatisticsEnabled             bool
	GroupName                     string
	Invalidate                    bool
	DefaultLockTimeout            int64
	MaxQueryIterators             int32
	Name                          string
	IsOnheapCacheEnabled          bool
	PartitionLossPolicy           int32
	QueryDetailMetricsSize        int32
	QueryParellelism              int32
	ReadFromBackup                bool
	RebalanceBatchSize            int32
	RebalanceBatchesPrefetchCount int64
	RebalanceDelay                int64
	RebalanceMode                 int32
	RebalanceOrder                int32
	RebalanceThrottle             int64
	RebalanceTimeout              int64
	SQLEscapeAll                  bool
	SQLIndexInlineMaxSize         int32
	SQLSchema                     string
	WriteSynchronizationMode      int32
	CacheKeyConfigurations        []CacheKeyConfiguration
	QueryEntities                 []QueryEntity
}

// CacheKeyConfiguration is struct
type CacheKeyConfiguration struct {
	TypeName     string
	KeyFieldName string
}

// QueryEntity is struct
type QueryEntity struct {
	KeyTypeName      string
	ValueTypeName    string
	TableName        string
	KeyFieldName     string
	ValueFieldName   string
	QueryFields      []QueryField
	FieldNameAliases []FieldNameAlias
	QueryIndexes     []QueryIndex
}

// QueryField is struct
type QueryField struct {
	Name                     string
	TypeName                 string
	IsKeyField               bool
	IsNotNullConstraintField bool
}

// QueryIndex is struct
type QueryIndex struct {
	Name       string
	Type       byte
	InlineSize int32
	Fields     []Field
}

// Field is struct
type Field struct {
	Name         string
	IsDescensing bool
}

// FieldNameAlias is struct
type FieldNameAlias struct {
	Name  string
	Alias string
}

func (c *client) CacheGetConfiguration(name string, flag byte, status *int32) (*CacheConfiguration, error) {
	return nil, fmt.Errorf("not implemented yet")
}
