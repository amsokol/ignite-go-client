package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

// Cache Configuration methods
// See for details:
// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations

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
func (c *client) CacheCreateWithName(cache string) error {
	r, err := c.Exec(OpCacheCreateWithName, cache)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_CREATE_WITH_NAME operation")
	}

	return r.CheckStatus()
}

// CacheGetOrCreateWithName creates a cache with a given name.
// Cache template can be applied if there is a '*' in the cache name.
// Does nothing if the cache exists.
func (c *client) CacheGetOrCreateWithName(cache string) error {
	r, err := c.Exec(OpCacheGetOrCreateWithName, cache)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_GET_OR_CREATE_WITH_NAME operation")
	}

	return r.CheckStatus()
}

// CacheGetNames returns existing cache names.
func (c *client) CacheGetNames() ([]string, error) {
	r, err := c.Exec(OpCacheGetNames)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_NAMES operation")
	}
	if err = r.CheckStatus(); err != nil {
		return nil, err
	}

	// get cache count
	var count int32
	if err := r.ReadPrimitives(&count); err != nil {
		return nil, errors.Wrapf(err, "failed to read cache count")
	}

	// read cache names
	names := make([]string, 0, int(count))
	for i := 0; i < int(count); i++ {
		var name string
		if err := r.ReadPrimitives(&name); err != nil {
			return nil, errors.Wrapf(err, "failed to read cache name with index %d", i)
		}
		names = append(names, name)
	}

	return names, nil
}

// CacheGetConfiguration gets configuration for the given cache.
func (c *client) CacheGetConfiguration(cache string, flag byte) (*CacheConfiguration, error) {
	r, err := c.Exec(OpCacheGetConfiguration, hashCode(cache), flag)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_CACHE_GET_CONFIGURATION operation")
	}
	if err = r.CheckStatus(); err != nil {
		return nil, err
	}

	var cc CacheConfiguration
	var count, length int32
	if err = r.ReadPrimitives(
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
		return nil, errors.Wrapf(err, "failed to read response data")
	}

	cc.CacheKeyConfigurations = make([]CacheKeyConfiguration, 0, int(count))
	for i := 0; i < int(count); i++ {
		var ckc CacheKeyConfiguration
		if err = r.ReadPrimitives(&ckc.TypeName, &ckc.AffinityKeyFieldName); err != nil {
			return nil, errors.Wrapf(err, "failed to read CacheKeyConfiguration data")
		}
		cc.CacheKeyConfigurations = append(cc.CacheKeyConfigurations, ckc)
	}

	if err = r.ReadPrimitives(&count); err != nil {
		return nil, errors.Wrapf(err, "failed to read QueryEntity count")
	}
	cc.QueryEntities = make([]QueryEntity, 0, int(count))
	for i := 0; i < int(count); i++ {
		var qe QueryEntity
		var count2 int32
		if err = r.ReadPrimitives(
			&qe.KeyTypeName,
			&qe.ValueTypeName,
			&qe.TableName,
			&qe.KeyFieldName,
			&qe.ValueFieldName,
			&count2); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryEntity data")
		}

		// read QueryField
		qe.QueryFields = make([]QueryField, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var qf QueryField
			if err = r.ReadPrimitives(
				&qf.Name,
				&qf.TypeName,
				&qf.IsKeyField,
				&qf.IsNotNullConstraintField); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryField data")
			}
			qe.QueryFields = append(qe.QueryFields, qf)
		}

		// read FieldNameAliases
		if err = r.ReadPrimitives(
			&count2); err != nil {
			return nil, errors.Wrapf(err, "failed to read FieldNameAlias count")
		}
		qe.FieldNameAliases = make([]FieldNameAlias, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var fna FieldNameAlias
			if err = r.ReadPrimitives(
				&fna.Name,
				&fna.Alias); err != nil {
				return nil, errors.Wrapf(err, "failed to read FieldNameAlias data")
			}
			qe.FieldNameAliases = append(qe.FieldNameAliases, fna)
		}

		// read QueryIndexes
		if err = r.ReadPrimitives(
			&count2); err != nil {
			return nil, errors.Wrapf(err, "failed to read QueryIndex count")
		}
		qe.QueryIndexes = make([]QueryIndex, 0, int(count2))
		for j := 0; j < int(count2); j++ {
			var qi QueryIndex
			var count3 int32
			if err = r.ReadPrimitives(
				&qi.Name,
				&qi.Type,
				&qi.InlineSize,
				&count3); err != nil {
				return nil, errors.Wrapf(err, "failed to read QueryIndex data")
			}

			// read Fields
			qi.Fields = make([]Field, 0, int(count3))
			for k := 0; k < int(count3); k++ {
				var f Field
				if err = r.ReadPrimitives(
					&f.Name,
					&f.IsDescensing); err != nil {
					return nil, errors.Wrapf(err, "failed to read Field data")
				}
				qi.Fields = append(qi.Fields, f)
			}
		}

		cc.QueryEntities = append(cc.QueryEntities, qe)
	}

	return &cc, nil
}

// CacheCreateWithConfiguration creates cache with provided configuration.
// An error is returned if the name is already in use.
func (c *client) CacheCreateWithConfiguration(cc *CacheConfigurationRefs) error {
	return c.cacheCreateWithConfiguration(OpCacheCreateWithConfiguration, cc)
}

func (c *client) cacheCreateWithConfiguration(code OperationCode, cc *CacheConfigurationRefs) error {
	o := c.Prepare(code)
	count := 0

	if cc.AtomicityMode != nil {
		if err := o.WritePrimitives(cacheConfigurationAtomicityModeCode, *cc.AtomicityMode); err != nil {
			return errors.Wrapf(err, "failed to write AtomicityMode property")
		}
		count++
	}
	if cc.Backups != nil {
		if err := o.WritePrimitives(cacheConfigurationBackupsCode, *cc.Backups); err != nil {
			return errors.Wrapf(err, "failed to write Backups property")
		}
		count++
	}
	if cc.CacheMode != nil {
		if err := o.WritePrimitives(cacheConfigurationCacheModeCode, *cc.CacheMode); err != nil {
			return errors.Wrapf(err, "failed to write CacheMode property")
		}
		count++
	}
	if cc.CopyOnRead != nil {
		if err := o.WritePrimitives(cacheConfigurationCopyOnReadCode, *cc.CopyOnRead); err != nil {
			return errors.Wrapf(err, "failed to write CopyOnRead property")
		}
		count++
	}
	if cc.DataRegionName != nil {
		if err := o.WritePrimitives(cacheConfigurationDataRegionNameCode, *cc.DataRegionName); err != nil {
			return errors.Wrapf(err, "failed to write DataRegionName property")
		}
		count++
	}
	if cc.EagerTTL != nil {
		if err := o.WritePrimitives(cacheConfigurationEagerTTLCode, *cc.EagerTTL); err != nil {
			return errors.Wrapf(err, "failed to write EagerTTL property")
		}
		count++
	}
	if cc.EnableStatistics != nil {
		if err := o.WritePrimitives(cacheConfigurationEnableStatisticsCode, *cc.EnableStatistics); err != nil {
			return errors.Wrapf(err, "failed to write EnableStatistics property")
		}
		count++
	}
	if cc.GroupName != nil {
		if err := o.WritePrimitives(cacheConfigurationGroupNameCode, *cc.GroupName); err != nil {
			return errors.Wrapf(err, "failed to write GroupName property")
		}
		count++
	}
	if cc.LockTimeout != nil {
		if err := o.WritePrimitives(cacheConfigurationLockTimeoutCode, *cc.LockTimeout); err != nil {
			return errors.Wrapf(err, "failed to write LockTimeout property")
		}
		count++
	}
	if cc.MaxConcurrentAsyncOperations != nil {
		if err := o.WritePrimitives(cacheConfigurationMaxConcurrentAsyncOperationsCode, *cc.MaxConcurrentAsyncOperations); err != nil {
			return errors.Wrapf(err, "failed to write MaxConcurrentAsyncOperations property")
		}
		count++
	}
	if cc.MaxQueryIterators != nil {
		if err := o.WritePrimitives(cacheConfigurationMaxQueryIteratorsCode, *cc.MaxQueryIterators); err != nil {
			return errors.Wrapf(err, "failed to write MaxQueryIterators property")
		}
		count++
	}
	if cc.Name != nil {
		if err := o.WritePrimitives(cacheConfigurationNameCode, *cc.Name); err != nil {
			return errors.Wrapf(err, "failed to write Name property")
		}
		count++
	}
	if cc.OnheapCacheEnabled != nil {
		if err := o.WritePrimitives(cacheConfigurationOnheapCacheEnabledCode, *cc.OnheapCacheEnabled); err != nil {
			return errors.Wrapf(err, "failed to write OnheapCacheEnabled property")
		}
		count++
	}
	if cc.PartitionLossPolicy != nil {
		if err := o.WritePrimitives(cacheConfigurationPartitionLossPolicyCode, *cc.PartitionLossPolicy); err != nil {
			return errors.Wrapf(err, "failed to write PartitionLossPolicy property")
		}
		count++
	}
	if cc.QueryDetailMetricsSize != nil {
		if err := o.WritePrimitives(cacheConfigurationQueryDetailMetricsSizeCode, *cc.QueryDetailMetricsSize); err != nil {
			return errors.Wrapf(err, "failed to write QueryDetailMetricsSize property")
		}
		count++
	}
	if cc.QueryParellelism != nil {
		if err := o.WritePrimitives(cacheConfigurationQueryParellelismCode, *cc.QueryParellelism); err != nil {
			return errors.Wrapf(err, "failed to write QueryParellelism property")
		}
		count++
	}
	if cc.ReadFromBackup != nil {
		if err := o.WritePrimitives(cacheConfigurationReadFromBackupCode, *cc.ReadFromBackup); err != nil {
			return errors.Wrapf(err, "failed to write ReadFromBackup property")
		}
		count++
	}
	if cc.RebalanceBatchSize != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceBatchSizeCode, *cc.RebalanceBatchSize); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceBatchSize property")
		}
		count++
	}
	if cc.RebalanceBatchesPrefetchCount != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceBatchesPrefetchCountCode, *cc.RebalanceBatchesPrefetchCount); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceBatchesPrefetchCount property")
		}
		count++
	}
	if cc.RebalanceDelay != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceDelayCode, *cc.RebalanceDelay); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceDelay property")
		}
		count++
	}
	if cc.RebalanceMode != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceModeCode, *cc.RebalanceMode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceMode property")
		}
		count++
	}
	if cc.RebalanceOrder != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceOrderCode, *cc.RebalanceOrder); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceOrder property")
		}
		count++
	}
	if cc.RebalanceThrottle != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceThrottleCode, *cc.RebalanceThrottle); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceThrottle property")
		}
		count++
	}
	if cc.RebalanceTimeout != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceTimeoutCode, *cc.RebalanceTimeout); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceTimeout property")
		}
		count++
	}
	if cc.SQLEscapeAll != nil {
		if err := o.WritePrimitives(cacheConfigurationSQLEscapeAllCode, *cc.SQLEscapeAll); err != nil {
			return errors.Wrapf(err, "failed to write SQLEscapeAll property")
		}
		count++
	}
	if cc.SQLIndexInlineMaxSize != nil {
		if err := o.WritePrimitives(cacheConfigurationSQLIndexInlineMaxSizeCode, *cc.SQLIndexInlineMaxSize); err != nil {
			return errors.Wrapf(err, "failed to write SQLIndexInlineMaxSize property")
		}
		count++
	}
	if cc.SQLSchema != nil {
		if err := o.WritePrimitives(cacheConfigurationSQLSchemaCode, *cc.SQLSchema); err != nil {
			return errors.Wrapf(err, "failed to write SQLSchema property")
		}
		count++
	}
	if cc.WriteSynchronizationMode != nil {
		if err := o.WritePrimitives(cacheConfigurationWriteSynchronizationModeCode, *cc.WriteSynchronizationMode); err != nil {
			return errors.Wrapf(err, "failed to write WriteSynchronizationMode property")
		}
		count++
	}
	if cc.WriteSynchronizationMode != nil {
		if err := o.WritePrimitives(cacheConfigurationWriteSynchronizationModeCode, *cc.WriteSynchronizationMode); err != nil {
			return errors.Wrapf(err, "failed to write WriteSynchronizationMode property")
		}
		count++
	}
	if cc.CacheKeyConfigurations != nil && len(cc.CacheKeyConfigurations) > 0 {
		if err := o.WritePrimitives(cacheConfigurationCacheKeyConfigurationsCode, int32(len(cc.CacheKeyConfigurations))); err != nil {
			return errors.Wrapf(err, "failed to write CacheKeyConfigurations code and count")
		}
		for i, v := range cc.CacheKeyConfigurations {
			if err := o.WritePrimitives(v.TypeName, v.AffinityKeyFieldName); err != nil {
				return errors.Wrapf(err, "failed to write CacheKeyConfiguration with index %d", i)
			}
		}
		count++
	}
	if cc.QueryEntities != nil && len(cc.QueryEntities) > 0 {
		if err := o.WritePrimitives(cacheConfigurationQueryEntitiesCode, int32(len(cc.QueryEntities))); err != nil {
			return errors.Wrapf(err, "failed to write QueryEntities code and count")
		}
		for i, v := range cc.QueryEntities {
			var l int32
			if v.QueryFields != nil {
				l = int32(len(v.QueryFields))
			}
			if err := o.WritePrimitives(v.KeyTypeName, v.ValueTypeName, v.TableName, v.KeyFieldName, v.ValueFieldName,
				l); err != nil {
				return errors.Wrapf(err, "failed to write QueryEntity with index %d", i)
			}
			if l > 0 {
				// write QueryFields
				for j, v2 := range v.QueryFields {
					if err := o.WritePrimitives(v2.Name, v2.TypeName, v2.IsKeyField, v2.IsNotNullConstraintField); err != nil {
						return errors.Wrapf(err, "failed to write QueryField with index %d", j)
					}
				}
			}
			// write FieldNameAliases
			l = 0
			if v.FieldNameAliases != nil {
				l = int32(len(v.FieldNameAliases))
			}
			if err := o.WritePrimitives(l); err != nil {
				return errors.Wrapf(err, "failed to write FieldNameAliases count")
			}
			if l > 0 {
				for j, v2 := range v.FieldNameAliases {
					if err := o.WritePrimitives(v2.Name, v2.Alias); err != nil {
						return errors.Wrapf(err, "failed to write FieldNameAlias with index %d", j)
					}
				}
			}
			// write QueryIndexes
			l = 0
			if v.QueryIndexes != nil {
				l = int32(len(v.QueryIndexes))
			}
			if err := o.WritePrimitives(l); err != nil {
				return errors.Wrapf(err, "failed to write QueryIndexes count")
			}
			if l > 0 {
				for j, v2 := range v.QueryIndexes {
					if err := o.WritePrimitives(v2.Name, v2.Type, v2.InlineSize); err != nil {
						return errors.Wrapf(err, "failed to write QueryIndex with index %d", j)
					}
					// write Fields
					l = 0
					if v2.Fields != nil {
						l = int32(len(v2.Fields))
					}
					if err := o.WritePrimitives(l); err != nil {
						return errors.Wrapf(err, "failed to write Fields count")
					}
					if l > 0 {
						for k, v3 := range v2.Fields {
							if err := o.WritePrimitives(v3.Name, v3.IsDescensing); err != nil {
								return errors.Wrapf(err, "failed to write Field with index %d", k)
							}
						}
					}
				}
			}
		}
		count++
	}

	if count == 0 {
		return errors.Errorf("no one property provided")
	}

	// execute
	if err := o.WritePrefix(int32(o.Data.Len()), int16(count)); err != nil {
		return errors.Wrapf(err, "failed to write message data length and property count")
	}
	r, err := c.Call(o)
	if err != nil {
		return errors.Wrapf(err, "failed to execute operation")
	}

	return r.CheckStatus()
}

// CacheGetOrCreateWithConfiguration creates cache with provided configuration.
// Does nothing if the name is already in use.
func (c *client) CacheGetOrCreateWithConfiguration(cc *CacheConfigurationRefs) error {
	return c.cacheCreateWithConfiguration(OpCacheGetOrCreateWithConfiguration, cc)
}

// CacheDestroy destroys cache with a given name.
func (c *client) CacheDestroy(cache string) error {
	r, err := c.Exec(OpCacheDestroy, hashCode(cache))
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_CACHE_DESTROY operation")
	}

	return r.CheckStatus()
}
