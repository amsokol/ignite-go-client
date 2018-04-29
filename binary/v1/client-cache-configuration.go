package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

const (
	cacheConfigurationAtomicityModeCode                 int16 = 2
	cacheConfigurationBackupsCode                       int16 = 3
	cacheConfigurationCacheModeCode                     int16 = 1
	cacheConfigurationCopyOnReadCode                    int16 = 5
	cacheConfigurationDataRegionNameCode                int16 = 100
	cacheConfigurationEagerTTLCode                      int16 = 405
	cacheConfigurationEnableStatisticsCode              int16 = 406
	cacheConfigurationGroupNameCode                     int16 = 400
	cacheConfigurationLockTimeoutCode                   int16 = 402
	cacheConfigurationMaxConcurrentAsyncOperationsCode  int16 = 403
	cacheConfigurationMaxQueryIteratorsCode             int16 = 206
	cacheConfigurationNameCode                          int16 = 0
	cacheConfigurationOnheapCacheEnabledCode            int16 = 101
	cacheConfigurationPartitionLossPolicyCode           int16 = 404
	cacheConfigurationQueryDetailMetricsSizeCode        int16 = 202
	cacheConfigurationQueryParellelismCode              int16 = 201
	cacheConfigurationReadFromBackupCode                int16 = 6
	cacheConfigurationRebalanceBatchSizeCode            int16 = 303
	cacheConfigurationRebalanceBatchesPrefetchCountCode int16 = 304
	cacheConfigurationRebalanceDelayCode                int16 = 301
	cacheConfigurationRebalanceModeCode                 int16 = 300
	cacheConfigurationRebalanceOrderCode                int16 = 305
	cacheConfigurationRebalanceThrottleCode             int16 = 306
	cacheConfigurationRebalanceTimeoutCode              int16 = 302
	cacheConfigurationSQLEscapeAllCode                  int16 = 205
	cacheConfigurationSQLIndexInlineMaxSizeCode         int16 = 204
	cacheConfigurationSQLSchemaCode                     int16 = 203
	cacheConfigurationWriteSynchronizationModeCode      int16 = 4
	cacheConfigurationCacheKeyConfigurationsCode        int16 = 401
	cacheConfigurationQueryEntitiesCode                 int16 = 200
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

// CacheConfigurationRefs describes cache configuration to create new cache
type CacheConfigurationRefs struct {
	AtomicityMode                 *int32
	Backups                       *int32
	CacheMode                     *int32
	CopyOnRead                    *bool
	DataRegionName                *string
	EagerTTL                      *bool
	EnableStatistics              *bool
	GroupName                     *string
	LockTimeout                   *int64
	MaxConcurrentAsyncOperations  *int32
	MaxQueryIterators             *int32
	Name                          *string
	OnheapCacheEnabled            *bool
	PartitionLossPolicy           *int32
	QueryDetailMetricsSize        *int32
	QueryParellelism              *int32
	ReadFromBackup                *bool
	RebalanceBatchSize            *int32
	RebalanceBatchesPrefetchCount *int64
	RebalanceDelay                *int64
	RebalanceMode                 *int32
	RebalanceOrder                *int32
	RebalanceThrottle             *int64
	RebalanceTimeout              *int64
	SQLEscapeAll                  *bool
	SQLIndexInlineMaxSize         *int32
	SQLSchema                     *string
	WriteSynchronizationMode      *int32
	CacheKeyConfigurations        []CacheKeyConfiguration
	QueryEntities                 []QueryEntity
}

// CacheCreateWithName Creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#section-op_cache_create_with_name
func (c *client) CacheCreateWithName(cache string) error {
	// request and response
	req := NewRequestOperation(OpCacheCreateWithName)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteOString(cache); err != nil {
		return errors.Wrapf(err, "failed to write cache name")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CREATE_WITH_NAME operation")
	}

	return res.CheckStatus()
}

// CacheGetOrCreateWithName creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
// Does nothing if the cache exists.
func (c *client) CacheGetOrCreateWithName(cache string) error {
	// request and response
	req := NewRequestOperation(OpCacheGetOrCreateWithName)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteOString(cache); err != nil {
		return errors.Wrapf(err, "failed to write cache name")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_GET_OR_CREATE_WITH_NAME operation")
	}

	return res.CheckStatus()
}

// CacheGetNames returns existing cache names.
func (c *client) CacheGetNames() ([]string, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetNames)
	res := NewResponseOperation(req.UID)

	// execute operation
	if err := c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_NAMES operation")
	}

	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	// get cache count
	count, err := res.ReadInt()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read cache name count")
	}

	// read cache names
	names := make([]string, 0, int(count))
	for i := 0; i < int(count); i++ {
		name, _, err := res.ReadOString()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read cache name with index %d", i)
		}
		names = append(names, name)
	}

	return names, nil
}

// CacheGetConfiguration gets configuration for the given cache.
func (c *client) CacheGetConfiguration(cache string, flag byte) (*CacheConfiguration, error) {
	// request and response
	req := NewRequestOperation(OpCacheGetConfiguration)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteByte(flag); err != nil {
		return nil, errors.Wrapf(err, "failed to write flag")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_CONFIGURATION operation")
	}

	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	// get cache configuration
	var err error
	var cc CacheConfiguration
	if _, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read length of the configuration in bytes")
	}
	if cc.AtomicityMode, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read AtomicityMode")
	}
	if cc.Backups, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read Backups")
	}
	if cc.CacheMode, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read CacheMode")
	}
	if cc.CopyOnRead, err = res.ReadBool(); err != nil {
		return nil, errors.Wrapf(err, "failed to read CopyOnRead")
	}
	if cc.DataRegionName, _, err = res.ReadOString(); err != nil {
		return nil, errors.Wrapf(err, "failed to read DataRegionName")
	}
	if cc.EagerTTL, err = res.ReadBool(); err != nil {
		return nil, errors.Wrapf(err, "failed to read EagerTTL")
	}
	if cc.EnableStatistics, err = res.ReadBool(); err != nil {
		return nil, errors.Wrapf(err, "failed to read EnableStatistics")
	}
	if cc.GroupName, _, err = res.ReadOString(); err != nil {
		return nil, errors.Wrapf(err, "failed to read GroupName")
	}
	if cc.LockTimeout, err = res.ReadLong(); err != nil {
		return nil, errors.Wrapf(err, "failed to read LockTimeout")
	}
	if cc.MaxConcurrentAsyncOperations, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read MaxConcurrentAsyncOperations")
	}
	if cc.MaxQueryIterators, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read MaxQueryIterators")
	}
	if cc.Name, _, err = res.ReadOString(); err != nil {
		return nil, errors.Wrapf(err, "failed to read Name")
	}
	if cc.OnheapCacheEnabled, err = res.ReadBool(); err != nil {
		return nil, errors.Wrapf(err, "failed to read OnheapCacheEnabled")
	}
	if cc.PartitionLossPolicy, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read PartitionLossPolicy")
	}
	if cc.QueryDetailMetricsSize, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read QueryDetailMetricsSize")
	}
	if cc.QueryParellelism, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read QueryParellelism")
	}
	if cc.ReadFromBackup, err = res.ReadBool(); err != nil {
		return nil, errors.Wrapf(err, "failed to read ReadFromBackup")
	}
	if cc.RebalanceBatchSize, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read RebalanceBatchSize")
	}
	if cc.RebalanceBatchesPrefetchCount, err = res.ReadLong(); err != nil {
		return nil, errors.Wrapf(err, "failed to read RebalanceBatchesPrefetchCount")
	}
	if cc.RebalanceDelay, err = res.ReadLong(); err != nil {
		return nil, errors.Wrapf(err, "failed to read RebalanceDelay")
	}
	if cc.RebalanceMode, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read RebalanceMode")
	}
	if cc.RebalanceOrder, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read RebalanceOrder")
	}
	if cc.RebalanceThrottle, err = res.ReadLong(); err != nil {
		return nil, errors.Wrapf(err, "failed to read RebalanceThrottle")
	}
	if cc.RebalanceTimeout, err = res.ReadLong(); err != nil {
		return nil, errors.Wrapf(err, "failed to read RebalanceTimeout")
	}
	if cc.SQLEscapeAll, err = res.ReadBool(); err != nil {
		return nil, errors.Wrapf(err, "failed to read SQLEscapeAll")
	}
	if cc.SQLIndexInlineMaxSize, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read SQLIndexInlineMaxSize")
	}
	if cc.SQLSchema, _, err = res.ReadOString(); err != nil {
		return nil, errors.Wrapf(err, "failed to read SQLSchema")
	}
	if cc.WriteSynchronizationMode, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read WriteSynchronizationMode")
	}
	// get CacheKeyConfiguration count
	var count int32
	if count, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read CacheKeyConfiguration count")
	}
	cc.CacheKeyConfigurations = make([]CacheKeyConfiguration, 0, int(count))
	for i := 0; i < int(count); i++ {
		var ckc CacheKeyConfiguration
		if ckc.TypeName, _, err = res.ReadOString(); err != nil {
			return nil, errors.Wrapf(err, "failed to read CacheKeyConfiguration.TypeName")
		}
		if ckc.AffinityKeyFieldName, _, err = res.ReadOString(); err != nil {
			return nil, errors.Wrapf(err, "failed to read CacheKeyConfiguration.AffinityKeyFieldName")
		}
		cc.CacheKeyConfigurations = append(cc.CacheKeyConfigurations, ckc)
	}
	// get QueryEntities count
	if count, err = res.ReadInt(); err != nil {
		return nil, errors.Wrapf(err, "failed to read QueryEntity count")
	}
	cc.QueryEntities = make([]QueryEntity, 0, int(count))
	for i := 0; i < int(count); i++ {
		var qe QueryEntity
		if qe.KeyTypeName, _, err = res.ReadOString(); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryEntity.KeyTypeName")
		}
		if qe.ValueTypeName, _, err = res.ReadOString(); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryEntity.ValueTypeName")
		}
		if qe.TableName, _, err = res.ReadOString(); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryEntity.TableName")
		}
		if qe.KeyFieldName, _, err = res.ReadOString(); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryEntity.KeyFieldName")
		}
		if qe.ValueFieldName, _, err = res.ReadOString(); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryEntity.ValueFieldName")
		}

		var count2 int32

		// read QueryFields
		if count2, err = res.ReadInt(); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryField count")
		}
		qe.QueryFields = make([]QueryField, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var qf QueryField
			if qf.Name, _, err = res.ReadOString(); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryField.Name")
			}
			if qf.TypeName, _, err = res.ReadOString(); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryField.TypeName")
			}
			if qf.IsKeyField, err = res.ReadBool(); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryField.IsKeyField")
			}
			if qf.IsNotNullConstraintField, err = res.ReadBool(); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryField.IsNotNullConstraintField")
			}
		}

		// read FieldNameAliases
		if count2, err = res.ReadInt(); err != nil {
			return nil, errors.Wrapf(err, "failed to read FieldNameAlias count")
		}
		qe.FieldNameAliases = make([]FieldNameAlias, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var fna FieldNameAlias
			if fna.Name, _, err = res.ReadOString(); err != nil {
				return nil, errors.Wrapf(err, "failed to read FieldNameAlias.Name")
			}
			if fna.Alias, _, err = res.ReadOString(); err != nil {
				return nil, errors.Wrapf(err, "failed to read FieldNameAlias.Alias")
			}
			qe.FieldNameAliases = append(qe.FieldNameAliases, fna)
		}

		// read QueryIndexes
		if count2, err = res.ReadInt(); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryIndex count")
		}
		qe.QueryIndexes = make([]QueryIndex, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var qi QueryIndex
			if qi.Name, _, err = res.ReadOString(); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryIndex.Name")
			}
			if qi.Type, err = res.ReadByte(); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryIndex.Type")
			}
			if qi.InlineSize, err = res.ReadInt(); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryIndex.InlineSize")
			}

			// read Fields
			var count3 int32
			if count3, err = res.ReadInt(); err != nil {
				return nil, errors.Wrapf(err, "failed to read Field count")
			}
			qi.Fields = make([]Field, 0, int(count3))
			for k := 0; k < int(count3); k++ {
				var f Field
				if f.Name, _, err = res.ReadOString(); err != nil {
					return nil, errors.Wrapf(err, "failed to read Field.Name")
				}
				if f.IsDescensing, err = res.ReadBool(); err != nil {
					return nil, errors.Wrapf(err, "failed to read Field.InlineSize")
				}
				qi.Fields = append(qi.Fields, f)
			}
		}

		cc.QueryEntities = append(cc.QueryEntities, qe)
	}

	return &cc, nil
}
