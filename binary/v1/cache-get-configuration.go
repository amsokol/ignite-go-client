package ignite

import (
	"fmt"
	"math/rand"
)

const (
	// CacheAtomicityModeTransactional is TRANSACTIONAL = 0
	CacheAtomicityModeTransactional = 0
	// CacheAtomicityModeAtomic is ATOMIC = 1
	CacheAtomicityModeAtomic = 1

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
	AtomicityMode                 int32
	Backups                       int32
	CacheMode                     int32
	CopyOnRead                    bool
	DataRegionName                string
	EagerTTL                      bool
	EnableStatistics              bool
	GroupName                     string
	LockTimeout                   int64
	MaxConcurrentAsyncOperations  int32
	MaxQueryIterators             int32
	Name                          string
	OnheapCacheEnabled            bool
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
	TypeName             string
	AffinityKeyFieldName string
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
	uid := rand.Int63()

	r, err := c.Exec(opCacheGetConfiguration, uid, hashCode(name), flag)
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

	var cc CacheConfiguration
	var count, length int32
	if err = r.Read(
		&length,
		&cc.AtomicityMode,
		&cc.Backups,
		&cc.CacheMode,
		&cc.CopyOnRead,
		&cc.DataRegionName,
		&cc.EagerTTL,
		&cc.EnableStatistics,
		&cc.GroupName,
		&cc.LockTimeout,
		&cc.MaxConcurrentAsyncOperations,
		&cc.MaxQueryIterators,
		&cc.Name,
		&cc.OnheapCacheEnabled,
		&cc.PartitionLossPolicy,
		&cc.QueryDetailMetricsSize,
		&cc.QueryParellelism,
		&cc.ReadFromBackup,
		&cc.RebalanceBatchSize,
		&cc.RebalanceBatchesPrefetchCount,
		&cc.RebalanceDelay,
		&cc.RebalanceMode,
		&cc.RebalanceOrder,
		&cc.RebalanceThrottle,
		&cc.RebalanceTimeout,
		&cc.SQLEscapeAll,
		&cc.SQLIndexInlineMaxSize,
		&cc.SQLSchema,
		&cc.WriteSynchronizationMode,
		&count); err != nil {
		return nil, fmt.Errorf("failed to read response data: %s", err.Error())
	}

	cc.CacheKeyConfigurations = make([]CacheKeyConfiguration, 0, int(count))
	for i := 0; i < int(count); i++ {
		var ckc CacheKeyConfiguration
		if err = r.Read(&ckc.TypeName, &ckc.AffinityKeyFieldName); err != nil {
			return nil, fmt.Errorf("failed to read CacheKeyConfiguration data: %s", err.Error())
		}
		cc.CacheKeyConfigurations = append(cc.CacheKeyConfigurations, ckc)
	}

	if err = r.Read(&count); err != nil {
		return nil, fmt.Errorf("failed to read QueryEntity count: %s", err.Error())
	}
	cc.QueryEntities = make([]QueryEntity, 0, int(count))
	for i := 0; i < int(count); i++ {
		var qe QueryEntity
		var count2 int32
		if err = r.Read(
			&qe.KeyTypeName,
			&qe.ValueTypeName,
			&qe.TableName,
			&qe.KeyFieldName,
			&qe.ValueFieldName,
			&count2); err != nil {
			return nil, fmt.Errorf("failed to read QueryEntity data: %s", err.Error())
		}

		// read QueryField
		qe.QueryFields = make([]QueryField, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var qf QueryField
			if err = r.Read(
				&qf.Name,
				&qf.TypeName,
				&qf.IsKeyField,
				&qf.IsNotNullConstraintField); err != nil {
				return nil, fmt.Errorf("failed to read QueryField data: %s", err.Error())
			}
			qe.QueryFields = append(qe.QueryFields, qf)
		}

		// read FieldNameAliases
		if err = r.Read(
			&count2); err != nil {
			return nil, fmt.Errorf("failed to read FieldNameAlias count: %s", err.Error())
		}
		qe.FieldNameAliases = make([]FieldNameAlias, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var fna FieldNameAlias
			if err = r.Read(
				&fna.Name,
				&fna.Alias); err != nil {
				return nil, fmt.Errorf("failed to read FieldNameAlias data: %s", err.Error())
			}
			qe.FieldNameAliases = append(qe.FieldNameAliases, fna)
		}

		// read QueryIndexes
		if err = r.Read(
			&count2); err != nil {
			return nil, fmt.Errorf("failed to read QueryIndex count: %s", err.Error())
		}
		qe.QueryIndexes = make([]QueryIndex, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var qi QueryIndex
			var count3 int32
			if err = r.Read(
				&qi.Name,
				&qi.Type,
				&qi.InlineSize,
				&count3); err != nil {
				return nil, fmt.Errorf("failed to read QueryIndex data: %s", err.Error())
			}

			// read Fields
			qi.Fields = make([]Field, 0, int(count3))
			for k := 0; k < int(count3); k++ {
				var f Field
				if err = r.Read(
					&f.Name,
					&f.IsDescensing); err != nil {
					return nil, fmt.Errorf("failed to read Field data: %s", err.Error())
				}
				qi.Fields = append(qi.Fields, f)
			}
		}

		cc.QueryEntities = append(cc.QueryEntities, qe)
	}

	return &cc, nil
}
