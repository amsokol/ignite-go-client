package ignite

import (
	"fmt"
	"math/rand"
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

// CacheCreateWithConfiguration creates cache with provided configuration. An exception is thrown if the name is already in use.
func (c *client) CacheCreateWithConfiguration(cc *CacheConfigurationRefs, status *int32) error {
	return c.cacheCreateWithConfiguration(opCacheCreateWithConfiguration, cc, status)
}

func (c *client) cacheCreateWithConfiguration(code int16, cc *CacheConfigurationRefs, status *int32) error {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()

	o := c.Prepare(code, uid)
	count := 0

	if cc.AtomicityMode != nil {
		if err := o.WritePrimitives(cacheConfigurationAtomicityModeCode, *cc.AtomicityMode); err != nil {
			return fmt.Errorf("failed to write AtomicityMode property: %s", err.Error())
		}
		count++
	}
	if cc.Backups != nil {
		if err := o.WritePrimitives(cacheConfigurationBackupsCode, *cc.Backups); err != nil {
			return fmt.Errorf("failed to write Backups property: %s", err.Error())
		}
		count++
	}
	if cc.CacheMode != nil {
		if err := o.WritePrimitives(cacheConfigurationCacheModeCode, *cc.CacheMode); err != nil {
			return fmt.Errorf("failed to write CacheMode property: %s", err.Error())
		}
		count++
	}
	if cc.CopyOnRead != nil {
		if err := o.WritePrimitives(cacheConfigurationCopyOnReadCode, *cc.CopyOnRead); err != nil {
			return fmt.Errorf("failed to write CopyOnRead property: %s", err.Error())
		}
		count++
	}
	if cc.DataRegionName != nil {
		if err := o.WritePrimitives(cacheConfigurationDataRegionNameCode, *cc.DataRegionName); err != nil {
			return fmt.Errorf("failed to write DataRegionName property: %s", err.Error())
		}
		count++
	}
	if cc.EagerTTL != nil {
		if err := o.WritePrimitives(cacheConfigurationEagerTTLCode, *cc.EagerTTL); err != nil {
			return fmt.Errorf("failed to write EagerTTL property: %s", err.Error())
		}
		count++
	}
	if cc.EnableStatistics != nil {
		if err := o.WritePrimitives(cacheConfigurationEnableStatisticsCode, *cc.EnableStatistics); err != nil {
			return fmt.Errorf("failed to write EnableStatistics property: %s", err.Error())
		}
		count++
	}
	if cc.GroupName != nil {
		if err := o.WritePrimitives(cacheConfigurationGroupNameCode, *cc.GroupName); err != nil {
			return fmt.Errorf("failed to write GroupName property: %s", err.Error())
		}
		count++
	}
	if cc.LockTimeout != nil {
		if err := o.WritePrimitives(cacheConfigurationLockTimeoutCode, *cc.LockTimeout); err != nil {
			return fmt.Errorf("failed to write LockTimeout property: %s", err.Error())
		}
		count++
	}
	if cc.MaxConcurrentAsyncOperations != nil {
		if err := o.WritePrimitives(cacheConfigurationMaxConcurrentAsyncOperationsCode, *cc.MaxConcurrentAsyncOperations); err != nil {
			return fmt.Errorf("failed to write MaxConcurrentAsyncOperations property: %s", err.Error())
		}
		count++
	}
	if cc.MaxQueryIterators != nil {
		if err := o.WritePrimitives(cacheConfigurationMaxQueryIteratorsCode, *cc.MaxQueryIterators); err != nil {
			return fmt.Errorf("failed to write MaxQueryIterators property: %s", err.Error())
		}
		count++
	}
	if cc.Name != nil {
		if err := o.WritePrimitives(cacheConfigurationNameCode, *cc.Name); err != nil {
			return fmt.Errorf("failed to write Name property: %s", err.Error())
		}
		count++
	}
	if cc.OnheapCacheEnabled != nil {
		if err := o.WritePrimitives(cacheConfigurationOnheapCacheEnabledCode, *cc.OnheapCacheEnabled); err != nil {
			return fmt.Errorf("failed to write OnheapCacheEnabled property: %s", err.Error())
		}
		count++
	}
	if cc.PartitionLossPolicy != nil {
		if err := o.WritePrimitives(cacheConfigurationPartitionLossPolicyCode, *cc.PartitionLossPolicy); err != nil {
			return fmt.Errorf("failed to write PartitionLossPolicy property: %s", err.Error())
		}
		count++
	}
	if cc.QueryDetailMetricsSize != nil {
		if err := o.WritePrimitives(cacheConfigurationQueryDetailMetricsSizeCode, *cc.QueryDetailMetricsSize); err != nil {
			return fmt.Errorf("failed to write QueryDetailMetricsSize property: %s", err.Error())
		}
		count++
	}
	if cc.QueryParellelism != nil {
		if err := o.WritePrimitives(cacheConfigurationQueryParellelismCode, *cc.QueryParellelism); err != nil {
			return fmt.Errorf("failed to write QueryParellelism property: %s", err.Error())
		}
		count++
	}
	if cc.ReadFromBackup != nil {
		if err := o.WritePrimitives(cacheConfigurationReadFromBackupCode, *cc.ReadFromBackup); err != nil {
			return fmt.Errorf("failed to write ReadFromBackup property: %s", err.Error())
		}
		count++
	}
	if cc.RebalanceBatchSize != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceBatchSizeCode, *cc.RebalanceBatchSize); err != nil {
			return fmt.Errorf("failed to write RebalanceBatchSize property: %s", err.Error())
		}
		count++
	}
	if cc.RebalanceBatchesPrefetchCount != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceBatchesPrefetchCountCode, *cc.RebalanceBatchesPrefetchCount); err != nil {
			return fmt.Errorf("failed to write RebalanceBatchesPrefetchCount property: %s", err.Error())
		}
		count++
	}
	if cc.RebalanceDelay != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceDelayCode, *cc.RebalanceDelay); err != nil {
			return fmt.Errorf("failed to write RebalanceDelay property: %s", err.Error())
		}
		count++
	}
	if cc.RebalanceMode != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceModeCode, *cc.RebalanceMode); err != nil {
			return fmt.Errorf("failed to write RebalanceMode property: %s", err.Error())
		}
		count++
	}
	if cc.RebalanceOrder != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceOrderCode, *cc.RebalanceOrder); err != nil {
			return fmt.Errorf("failed to write RebalanceOrder property: %s", err.Error())
		}
		count++
	}
	if cc.RebalanceThrottle != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceThrottleCode, *cc.RebalanceThrottle); err != nil {
			return fmt.Errorf("failed to write RebalanceThrottle property: %s", err.Error())
		}
		count++
	}
	if cc.RebalanceTimeout != nil {
		if err := o.WritePrimitives(cacheConfigurationRebalanceTimeoutCode, *cc.RebalanceTimeout); err != nil {
			return fmt.Errorf("failed to write RebalanceTimeout property: %s", err.Error())
		}
		count++
	}
	if cc.SQLEscapeAll != nil {
		if err := o.WritePrimitives(cacheConfigurationSQLEscapeAllCode, *cc.SQLEscapeAll); err != nil {
			return fmt.Errorf("failed to write SQLEscapeAll property: %s", err.Error())
		}
		count++
	}
	if cc.SQLIndexInlineMaxSize != nil {
		if err := o.WritePrimitives(cacheConfigurationSQLIndexInlineMaxSizeCode, *cc.SQLIndexInlineMaxSize); err != nil {
			return fmt.Errorf("failed to write SQLIndexInlineMaxSize property: %s", err.Error())
		}
		count++
	}
	if cc.SQLSchema != nil {
		if err := o.WritePrimitives(cacheConfigurationSQLSchemaCode, *cc.SQLSchema); err != nil {
			return fmt.Errorf("failed to write SQLSchema property: %s", err.Error())
		}
		count++
	}
	if cc.WriteSynchronizationMode != nil {
		if err := o.WritePrimitives(cacheConfigurationWriteSynchronizationModeCode, *cc.WriteSynchronizationMode); err != nil {
			return fmt.Errorf("failed to write WriteSynchronizationMode property: %s", err.Error())
		}
		count++
	}
	if cc.WriteSynchronizationMode != nil {
		if err := o.WritePrimitives(cacheConfigurationWriteSynchronizationModeCode, *cc.WriteSynchronizationMode); err != nil {
			return fmt.Errorf("failed to write WriteSynchronizationMode property: %s", err.Error())
		}
		count++
	}
	if cc.CacheKeyConfigurations != nil && len(cc.CacheKeyConfigurations) > 0 {
		if err := o.WritePrimitives(cacheConfigurationCacheKeyConfigurationsCode, int32(len(cc.CacheKeyConfigurations))); err != nil {
			return fmt.Errorf("failed to write CacheKeyConfigurations code and count: %s", err.Error())
		}
		for i, v := range cc.CacheKeyConfigurations {
			if err := o.WritePrimitives(v.TypeName, v.AffinityKeyFieldName); err != nil {
				return fmt.Errorf("failed to write CacheKeyConfiguration with index %d: %s", i, err.Error())
			}
		}
		count++
	}
	if cc.QueryEntities != nil && len(cc.QueryEntities) > 0 {
		if err := o.WritePrimitives(cacheConfigurationQueryEntitiesCode, int32(len(cc.QueryEntities))); err != nil {
			return fmt.Errorf("failed to write QueryEntities code and count: %s", err.Error())
		}
		for i, v := range cc.QueryEntities {
			var l int32
			if v.QueryFields != nil {
				l = int32(len(v.QueryFields))
			}
			if err := o.WritePrimitives(v.KeyTypeName, v.ValueTypeName, v.TableName, v.KeyFieldName, v.ValueFieldName,
				l); err != nil {
				return fmt.Errorf("failed to write QueryEntity with index %d: %s", i, err.Error())
			}
			if l > 0 {
				// write QueryFields
				for j, v2 := range v.QueryFields {
					if err := o.WritePrimitives(v2.Name, v2.TypeName, v2.IsKeyField, v2.IsNotNullConstraintField); err != nil {
						return fmt.Errorf("failed to write QueryField with index %d: %s", j, err.Error())
					}
				}
			}
			// write FieldNameAliases
			l = 0
			if v.FieldNameAliases != nil {
				l = int32(len(v.FieldNameAliases))
			}
			if err := o.WritePrimitives(l); err != nil {
				return fmt.Errorf("failed to write FieldNameAliases count: %s", err.Error())
			}
			if l > 0 {
				for j, v2 := range v.FieldNameAliases {
					if err := o.WritePrimitives(v2.Name, v2.Alias); err != nil {
						return fmt.Errorf("failed to write FieldNameAlias with index %d: %s", j, err.Error())
					}
				}
			}
			// write QueryIndexes
			l = 0
			if v.QueryIndexes != nil {
				l = int32(len(v.QueryIndexes))
			}
			if err := o.WritePrimitives(l); err != nil {
				return fmt.Errorf("failed to write QueryIndexes count: %s", err.Error())
			}
			if l > 0 {
				for j, v2 := range v.QueryIndexes {
					if err := o.WritePrimitives(v2.Name, v2.Type, v2.InlineSize); err != nil {
						return fmt.Errorf("failed to write QueryIndex with index %d: %s", j, err.Error())
					}
					// write Fields
					l = 0
					if v2.Fields != nil {
						l = int32(len(v2.Fields))
					}
					if err := o.WritePrimitives(l); err != nil {
						return fmt.Errorf("failed to write Fields count: %s", err.Error())
					}
					if l > 0 {
						for k, v3 := range v2.Fields {
							if err := o.WritePrimitives(v3.Name, v3.IsDescensing); err != nil {
								return fmt.Errorf("failed to write Field with index %d: %s", k, err.Error())
							}
						}
					}
				}
			}
		}
		count++
	}

	if count == 0 {
		return fmt.Errorf("no one property provided")
	}

	// execute
	if err := o.WritePrefix(int32(o.Data.Len()), int16(count)); err != nil {
		return fmt.Errorf("failed to write message data length and property count: %s", err.Error())
	}
	r, err := c.Call(o)
	if err != nil {
		return fmt.Errorf("failed to execute operation: %s", err.Error())
	}
	if r.UID != uid {
		return fmt.Errorf("invalid response id (expected %d, but received %d)", uid, r.UID)
	}
	if status != nil {
		*status = r.Status
	}
	if r.Status != StatusSuccess {
		return fmt.Errorf("failed to execute operation: status=%d, message=%s", r.Status, r.Message)
	}

	return nil
}
