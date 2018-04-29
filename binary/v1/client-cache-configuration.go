package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

const (
	cacheConfigurationAtomicityModeCode                 = 2
	cacheConfigurationBackupsCode                       = 3
	cacheConfigurationCacheModeCode                     = 1
	cacheConfigurationCopyOnReadCode                    = 5
	cacheConfigurationDataRegionNameCode                = 100
	cacheConfigurationEagerTTLCode                      = 405
	cacheConfigurationEnableStatisticsCode              = 406
	cacheConfigurationGroupNameCode                     = 400
	cacheConfigurationLockTimeoutCode                   = 402
	cacheConfigurationMaxConcurrentAsyncOperationsCode  = 403
	cacheConfigurationMaxQueryIteratorsCode             = 206
	cacheConfigurationNameCode                          = 0
	cacheConfigurationOnheapCacheEnabledCode            = 101
	cacheConfigurationPartitionLossPolicyCode           = 404
	cacheConfigurationQueryDetailMetricsSizeCode        = 202
	cacheConfigurationQueryParellelismCode              = 201
	cacheConfigurationReadFromBackupCode                = 6
	cacheConfigurationRebalanceBatchSizeCode            = 303
	cacheConfigurationRebalanceBatchesPrefetchCountCode = 304
	cacheConfigurationRebalanceDelayCode                = 301
	cacheConfigurationRebalanceModeCode                 = 300
	cacheConfigurationRebalanceOrderCode                = 305
	cacheConfigurationRebalanceThrottleCode             = 306
	cacheConfigurationRebalanceTimeoutCode              = 302
	cacheConfigurationSQLEscapeAllCode                  = 205
	cacheConfigurationSQLIndexInlineMaxSizeCode         = 204
	cacheConfigurationSQLSchemaCode                     = 203
	cacheConfigurationWriteSynchronizationModeCode      = 4
	cacheConfigurationCacheKeyConfigurationsCode        = 401
	cacheConfigurationQueryEntitiesCode                 = 200
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

// CacheCreateWithConfiguration creates cache with provided configuration.
// An error is returned if the name is already in use.
func (c *client) CacheCreateWithConfiguration(cc *CacheConfigurationRefs) error {
	return c.cacheCreateWithConfiguration(OpCacheCreateWithConfiguration, cc)
}

// CacheGetOrCreateWithConfiguration creates cache with provided configuration.
// Does nothing if the name is already in use.
func (c *client) CacheGetOrCreateWithConfiguration(cc *CacheConfigurationRefs) error {
	return c.cacheCreateWithConfiguration(OpCacheGetOrCreateWithConfiguration, cc)
}

func (c *client) cacheCreateWithConfiguration(code int16, cc *CacheConfigurationRefs) error {
	// request and response
	req := NewRequestCacheCreateWithConfiguration(code)
	res := NewResponseOperation(req.UID)

	if cc.AtomicityMode != nil {
		if err := req.WriteShort(cacheConfigurationAtomicityModeCode); err != nil {
			return errors.Wrapf(err, "failed to write AtomicityMode property code")
		}
		if err := req.WriteInt(*cc.AtomicityMode); err != nil {
			return errors.Wrapf(err, "failed to write AtomicityMode property value")
		}
		req.Count++
	}
	if cc.Backups != nil {
		if err := req.WriteShort(cacheConfigurationBackupsCode); err != nil {
			return errors.Wrapf(err, "failed to write Backups property code")
		}
		if err := req.WriteInt(*cc.Backups); err != nil {
			return errors.Wrapf(err, "failed to write Backups property value")
		}
		req.Count++
	}
	if cc.CacheMode != nil {
		if err := req.WriteShort(cacheConfigurationCacheModeCode); err != nil {
			return errors.Wrapf(err, "failed to write CacheMode property code")
		}
		if err := req.WriteInt(*cc.CacheMode); err != nil {
			return errors.Wrapf(err, "failed to write CacheMode property value")
		}
		req.Count++
	}
	if cc.CopyOnRead != nil {
		if err := req.WriteShort(cacheConfigurationCopyOnReadCode); err != nil {
			return errors.Wrapf(err, "failed to write CopyOnRead property code")
		}
		if err := req.WriteBool(*cc.CopyOnRead); err != nil {
			return errors.Wrapf(err, "failed to write CopyOnRead property value")
		}
		req.Count++
	}
	if cc.DataRegionName != nil {
		if err := req.WriteShort(cacheConfigurationDataRegionNameCode); err != nil {
			return errors.Wrapf(err, "failed to write DataRegionName property code")
		}
		if err := req.WriteOString(*cc.DataRegionName); err != nil {
			return errors.Wrapf(err, "failed to write DataRegionName property value")
		}
		req.Count++
	}
	if cc.EagerTTL != nil {
		if err := req.WriteShort(cacheConfigurationEagerTTLCode); err != nil {
			return errors.Wrapf(err, "failed to write EagerTTL property code")
		}
		if err := req.WriteBool(*cc.EagerTTL); err != nil {
			return errors.Wrapf(err, "failed to write EagerTTL property value")
		}
		req.Count++
	}
	if cc.EnableStatistics != nil {
		if err := req.WriteShort(cacheConfigurationEnableStatisticsCode); err != nil {
			return errors.Wrapf(err, "failed to write EnableStatistics property code")
		}
		if err := req.WriteBool(*cc.EnableStatistics); err != nil {
			return errors.Wrapf(err, "failed to write EnableStatistics property value")
		}
		req.Count++
	}
	if cc.GroupName != nil {
		if err := req.WriteShort(cacheConfigurationGroupNameCode); err != nil {
			return errors.Wrapf(err, "failed to write GroupName property code")
		}
		if err := req.WriteOString(*cc.GroupName); err != nil {
			return errors.Wrapf(err, "failed to write GroupName property value")
		}
		req.Count++
	}
	if cc.LockTimeout != nil {
		if err := req.WriteShort(cacheConfigurationLockTimeoutCode); err != nil {
			return errors.Wrapf(err, "failed to write LockTimeout property code")
		}
		if err := req.WriteLong(*cc.LockTimeout); err != nil {
			return errors.Wrapf(err, "failed to write LockTimeout property value")
		}
		req.Count++
	}
	if cc.MaxConcurrentAsyncOperations != nil {
		if err := req.WriteShort(cacheConfigurationMaxConcurrentAsyncOperationsCode); err != nil {
			return errors.Wrapf(err, "failed to write MaxConcurrentAsyncOperations property code")
		}
		if err := req.WriteInt(*cc.MaxConcurrentAsyncOperations); err != nil {
			return errors.Wrapf(err, "failed to write MaxConcurrentAsyncOperations property value")
		}
		req.Count++
	}
	if cc.MaxQueryIterators != nil {
		if err := req.WriteShort(cacheConfigurationMaxQueryIteratorsCode); err != nil {
			return errors.Wrapf(err, "failed to write MaxQueryIterators property code")
		}
		if err := req.WriteInt(*cc.MaxQueryIterators); err != nil {
			return errors.Wrapf(err, "failed to write MaxQueryIterators property value")
		}
		req.Count++
	}
	if cc.Name != nil {
		if err := req.WriteShort(cacheConfigurationNameCode); err != nil {
			return errors.Wrapf(err, "failed to write Name property code")
		}
		if err := req.WriteOString(*cc.Name); err != nil {
			return errors.Wrapf(err, "failed to write Name property value")
		}
		req.Count++
	}
	if cc.OnheapCacheEnabled != nil {
		if err := req.WriteShort(cacheConfigurationOnheapCacheEnabledCode); err != nil {
			return errors.Wrapf(err, "failed to write OnheapCacheEnabled property code")
		}
		if err := req.WriteBool(*cc.OnheapCacheEnabled); err != nil {
			return errors.Wrapf(err, "failed to write OnheapCacheEnabled property value")
		}
		req.Count++
	}
	if cc.PartitionLossPolicy != nil {
		if err := req.WriteShort(cacheConfigurationPartitionLossPolicyCode); err != nil {
			return errors.Wrapf(err, "failed to write PartitionLossPolicy property code")
		}
		if err := req.WriteInt(*cc.PartitionLossPolicy); err != nil {
			return errors.Wrapf(err, "failed to write PartitionLossPolicy property value")
		}
		req.Count++
	}
	if cc.QueryDetailMetricsSize != nil {
		if err := req.WriteShort(cacheConfigurationQueryDetailMetricsSizeCode); err != nil {
			return errors.Wrapf(err, "failed to write QueryDetailMetricsSize property code")
		}
		if err := req.WriteInt(*cc.QueryDetailMetricsSize); err != nil {
			return errors.Wrapf(err, "failed to write QueryDetailMetricsSize property value")
		}
		req.Count++
	}
	if cc.QueryParellelism != nil {
		if err := req.WriteShort(cacheConfigurationQueryParellelismCode); err != nil {
			return errors.Wrapf(err, "failed to write QueryParellelism property code")
		}
		if err := req.WriteInt(*cc.QueryParellelism); err != nil {
			return errors.Wrapf(err, "failed to write QueryParellelism property value")
		}
		req.Count++
	}
	if cc.ReadFromBackup != nil {
		if err := req.WriteShort(cacheConfigurationReadFromBackupCode); err != nil {
			return errors.Wrapf(err, "failed to write ReadFromBackup property code")
		}
		if err := req.WriteBool(*cc.ReadFromBackup); err != nil {
			return errors.Wrapf(err, "failed to write ReadFromBackup property value")
		}
		req.Count++
	}
	if cc.RebalanceBatchSize != nil {
		if err := req.WriteShort(cacheConfigurationRebalanceBatchSizeCode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceBatchSize property code")
		}
		if err := req.WriteInt(*cc.RebalanceBatchSize); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceBatchSize property value")
		}
		req.Count++
	}
	if cc.RebalanceBatchesPrefetchCount != nil {
		if err := req.WriteShort(cacheConfigurationRebalanceBatchesPrefetchCountCode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceBatchesPrefetchCount property code")
		}
		if err := req.WriteLong(*cc.RebalanceBatchesPrefetchCount); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceBatchesPrefetchCount property value")
		}
		req.Count++
	}
	if cc.RebalanceDelay != nil {
		if err := req.WriteShort(cacheConfigurationRebalanceDelayCode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceDelay property code")
		}
		if err := req.WriteLong(*cc.RebalanceDelay); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceDelay property value")
		}
		req.Count++
	}
	if cc.RebalanceMode != nil {
		if err := req.WriteShort(cacheConfigurationRebalanceModeCode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceMode property code")
		}
		if err := req.WriteInt(*cc.RebalanceMode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceMode property value")
		}
		req.Count++
	}
	if cc.RebalanceOrder != nil {
		if err := req.WriteShort(cacheConfigurationRebalanceOrderCode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceOrder property code")
		}
		if err := req.WriteInt(*cc.RebalanceOrder); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceOrder property value")
		}
		req.Count++
	}
	if cc.RebalanceThrottle != nil {
		if err := req.WriteShort(cacheConfigurationRebalanceThrottleCode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceThrottle property code")
		}
		if err := req.WriteLong(*cc.RebalanceThrottle); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceThrottle property value")
		}
		req.Count++
	}
	if cc.RebalanceTimeout != nil {
		if err := req.WriteShort(cacheConfigurationRebalanceTimeoutCode); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceTimeout property code")
		}
		if err := req.WriteLong(*cc.RebalanceTimeout); err != nil {
			return errors.Wrapf(err, "failed to write RebalanceTimeout property value")
		}
		req.Count++
	}
	if cc.SQLEscapeAll != nil {
		if err := req.WriteShort(cacheConfigurationSQLEscapeAllCode); err != nil {
			return errors.Wrapf(err, "failed to write SQLEscapeAll property code")
		}
		if err := req.WriteBool(*cc.SQLEscapeAll); err != nil {
			return errors.Wrapf(err, "failed to write SQLEscapeAll property value")
		}
		req.Count++
	}
	if cc.SQLIndexInlineMaxSize != nil {
		if err := req.WriteShort(cacheConfigurationSQLIndexInlineMaxSizeCode); err != nil {
			return errors.Wrapf(err, "failed to write SQLIndexInlineMaxSize property code")
		}
		if err := req.WriteInt(*cc.SQLIndexInlineMaxSize); err != nil {
			return errors.Wrapf(err, "failed to write SQLIndexInlineMaxSize property value")
		}
		req.Count++
	}
	if cc.SQLSchema != nil {
		if err := req.WriteShort(cacheConfigurationSQLSchemaCode); err != nil {
			return errors.Wrapf(err, "failed to write SQLSchema property code")
		}
		if err := req.WriteOString(*cc.SQLSchema); err != nil {
			return errors.Wrapf(err, "failed to write SQLSchema property value")
		}
		req.Count++
	}
	if cc.WriteSynchronizationMode != nil {
		if err := req.WriteShort(cacheConfigurationWriteSynchronizationModeCode); err != nil {
			return errors.Wrapf(err, "failed to write WriteSynchronizationMode property code")
		}
		if err := req.WriteInt(*cc.WriteSynchronizationMode); err != nil {
			return errors.Wrapf(err, "failed to write WriteSynchronizationMode property value")
		}
		req.Count++
	}
	if cc.WriteSynchronizationMode != nil {
		if err := req.WriteShort(cacheConfigurationWriteSynchronizationModeCode); err != nil {
			return errors.Wrapf(err, "failed to write WriteSynchronizationMode property code")
		}
		if err := req.WriteInt(*cc.WriteSynchronizationMode); err != nil {
			return errors.Wrapf(err, "failed to write WriteSynchronizationMode property value")
		}
		req.Count++
	}
	if cc.CacheKeyConfigurations != nil && len(cc.CacheKeyConfigurations) > 0 {
		if err := req.WriteShort(cacheConfigurationCacheKeyConfigurationsCode); err != nil {
			return errors.Wrapf(err, "failed to write CacheKeyConfigurations code")
		}
		if err := req.WriteInt(int32(len(cc.CacheKeyConfigurations))); err != nil {
			return errors.Wrapf(err, "failed to write CacheKeyConfigurations count")
		}
		for i, v := range cc.CacheKeyConfigurations {
			if err := req.WriteOString(v.TypeName); err != nil {
				return errors.Wrapf(err, "failed to write CacheKeyConfiguration.TypeName with index %d", i)
			}
			if err := req.WriteOString(v.AffinityKeyFieldName); err != nil {
				return errors.Wrapf(err, "failed to write CacheKeyConfiguration.AffinityKeyFieldName with index %d", i)
			}
		}
		req.Count++
	}
	if cc.QueryEntities != nil && len(cc.QueryEntities) > 0 {
		if err := req.WriteShort(cacheConfigurationQueryEntitiesCode); err != nil {
			return errors.Wrapf(err, "failed to write QueryEntities code")
		}
		if err := req.WriteInt(int32(len(cc.QueryEntities))); err != nil {
			return errors.Wrapf(err, "failed to write QueryEntity count")
		}
		for i, v := range cc.QueryEntities {
			if err := req.WriteOString(v.KeyTypeName); err != nil {
				return errors.Wrapf(err, "failed to write QueryEntity.KeyTypeName with index %d", i)
			}
			if err := req.WriteOString(v.ValueTypeName); err != nil {
				return errors.Wrapf(err, "failed to write QueryEntity.ValueTypeName with index %d", i)
			}
			if err := req.WriteOString(v.TableName); err != nil {
				return errors.Wrapf(err, "failed to write QueryEntity.TableName with index %d", i)
			}
			if err := req.WriteOString(v.KeyFieldName); err != nil {
				return errors.Wrapf(err, "failed to write QueryEntity.KeyFieldName with index %d", i)
			}
			if err := req.WriteOString(v.ValueFieldName); err != nil {
				return errors.Wrapf(err, "failed to write QueryEntity.ValueFieldName with index %d", i)
			}
			var l int32
			if v.QueryFields != nil {
				l = int32(len(v.QueryFields))
			}
			if err := req.WriteInt(l); err != nil {
				return errors.Wrapf(err, "failed to write QueryField count")
			}
			if l > 0 {
				// write QueryFields
				for j, v2 := range v.QueryFields {
					if err := req.WriteOString(v2.Name); err != nil {
						return errors.Wrapf(err, "failed to write QueryField.Name with index %d", j)
					}
					if err := req.WriteOString(v2.TypeName); err != nil {
						return errors.Wrapf(err, "failed to write QueryField.TypeName with index %d", j)
					}
					if err := req.WriteBool(v2.IsKeyField); err != nil {
						return errors.Wrapf(err, "failed to write QueryField.IsKeyField with index %d", j)
					}
					if err := req.WriteBool(v2.IsNotNullConstraintField); err != nil {
						return errors.Wrapf(err, "failed to write QueryField.IsNotNullConstraintField with index %d", j)
					}
				}
			}
			// write FieldNameAliases
			l = 0
			if v.FieldNameAliases != nil {
				l = int32(len(v.FieldNameAliases))
			}
			if err := req.WriteInt(l); err != nil {
				return errors.Wrapf(err, "failed to write FieldNameAlias count")
			}
			if l > 0 {
				for j, v2 := range v.FieldNameAliases {
					if err := req.WriteOString(v2.Name); err != nil {
						return errors.Wrapf(err, "failed to write FieldNameAlias.Name with index %d", j)
					}
					if err := req.WriteOString(v2.Alias); err != nil {
						return errors.Wrapf(err, "failed to write FieldNameAlias.Alias with index %d", j)
					}
				}
			}
			// write QueryIndexes
			l = 0
			if v.QueryIndexes != nil {
				l = int32(len(v.QueryIndexes))
			}
			if err := req.WriteInt(l); err != nil {
				return errors.Wrapf(err, "failed to write QueryIndex count")
			}
			if l > 0 {
				for j, v2 := range v.QueryIndexes {
					if err := req.WriteOString(v2.Name); err != nil {
						return errors.Wrapf(err, "failed to write QueryIndex.Name with index %d", j)
					}
					if err := req.WriteByte(v2.Type); err != nil {
						return errors.Wrapf(err, "failed to write QueryIndex.Type with index %d", j)
					}
					if err := req.WriteInt(v2.InlineSize); err != nil {
						return errors.Wrapf(err, "failed to write QueryIndex.InlineSize with index %d", j)
					}
					// write Fields
					l = 0
					if v2.Fields != nil {
						l = int32(len(v2.Fields))
					}
					if err := req.WriteInt(l); err != nil {
						return errors.Wrapf(err, "failed to write Field count")
					}
					if l > 0 {
						for k, v3 := range v2.Fields {
							if err := req.WriteOString(v3.Name); err != nil {
								return errors.Wrapf(err, "failed to write Field.Name with index %d", k)
							}
							if err := req.WriteBool(v3.IsDescensing); err != nil {
								return errors.Wrapf(err, "failed to write Field.IsDescensing with index %d", k)
							}
						}
					}
				}
			}
		}
		req.Count++
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute operation to create cache with configuration")
	}

	return res.CheckStatus()
}
