package ignite

const (
	// Cache Configuration

	// OpCacheGetNames gets existing cache names.
	OpCacheGetNames = 1050
	// OpCacheCreateWithName creates a cache with a given name.
	OpCacheCreateWithName = 1051
	// OpCacheGetOrCreateWithName creates a cache with a given name.
	// Does nothing if the cache exists.
	OpCacheGetOrCreateWithName = 1052
	// OpCacheCreateWithConfiguration creates cache with provided configuration.
	OpCacheCreateWithConfiguration = 1053
	// OpCacheGetOrCreateWithConfiguration creates cache with provided configuration.
	// Does nothing if the name is already in use.
	OpCacheGetOrCreateWithConfiguration = 1054
	// OpCacheGetConfiguration gets configuration for the given cache.
	OpCacheGetConfiguration = 1055
	// OpCacheDestroy destroys cache with a given name.
	OpCacheDestroy = 1056

	// Key-Value Queries

	// OpCacheGet retrieves a value from cache by key.
	OpCacheGet = 1000
	// OpCachePut puts a value with a given key to cache (overwriting existing value if any).
	OpCachePut = 1001
	// OpCachePutIfAbsent puts a value with a given key to cache only if the key does not already exist.
	OpCachePutIfAbsent = 1002
	// OpCacheGetAll retrieves multiple key-value pairs from cache.
	OpCacheGetAll = 1003
	// OpCachePutAll puts multiple key-value pairs to cache (overwriting existing associations if any).
	OpCachePutAll = 1004
	// OpCacheGetAndPut puts a value with a given key to cache, and returns the previous value for that key.
	OpCacheGetAndPut = 1005
	// OpCacheGetAndReplace puts a value with a given key to cache, returning previous value for that key,
	// if and only if there is a value currently mapped for that key.
	OpCacheGetAndReplace = 1006
	// OpCacheGetAndRemove removes the cache entry with specified key, returning the value.
	OpCacheGetAndRemove = 1007
	// OpCacheGetAndPutIfAbsent puts a value with a given key to cache only if the key does not already exist.
	OpCacheGetAndPutIfAbsent = 1008
	// OpCacheReplace puts a value with a given key to cache only if the key already exists.
	OpCacheReplace = 1009
	// OpCacheReplaceIfEquals puts a value with a given key to cache only if the key already exists and value equals provided value.
	OpCacheReplaceIfEquals = 1010
	// OpCacheContainsKey returns a value indicating whether given key is present in cache.
	OpCacheContainsKey = 1011
	// OpCacheContainsKeys returns a value indicating whether all given keys are present in cache.
	OpCacheContainsKeys = 1012
	// OpCacheClear clears the cache without notifying listeners or cache writers.
	OpCacheClear = 1013
	// OpCacheClearKey clears the cache key without notifying listeners or cache writers.
	OpCacheClearKey = 1014
	// OpCacheClearKeys clears the cache keys without notifying listeners or cache writers.
	OpCacheClearKeys = 1015
	// OpCacheRemoveKey removes an entry with a given key, notifying listeners and cache writers.
	OpCacheRemoveKey = 1016
	// OpCacheRemoveIfEquals removes an entry with a given key if provided value is equal to actual value,
	// notifying listeners and cache writers.
	OpCacheRemoveIfEquals = 1017
	// OpCacheRemoveKeys removes entries with given keys, notifying listeners and cache writers.
	OpCacheRemoveKeys = 1018
	// OpCacheRemoveAll removes all entries from cache, notifying listeners and cache writers.
	OpCacheRemoveAll = 1019
	// OpCacheGetSize gets the number of entries in cache.
	OpCacheGetSize = 1020

	// SQL and Scan Queries

	// OpQuerySQL executes an SQL query over data stored in the cluster.
	// The query returns the whole record (key and value).
	OpQuerySQL = 2002
	// OpQuerySQLCursorGetPage retrieves the next SQL query cursor page by cursor id from OP_QUERY_SQL.
	OpQuerySQLCursorGetPage = 2003
	// OpQuerySQLFields performs SQL fields query.
	OpQuerySQLFields = 2004
	// OpQuerySQLFieldsCursorGetPage retrieves the next query result page by cursor id from OP_QUERY_SQL_FIELDS.
	OpQuerySQLFieldsCursorGetPage = 2005
	// OpQueryScan performs scan query.
	OpQueryScan = 2000
	// OpQueryScanCursorGetPage fetches the next SQL query cursor page by cursor id that is obtained from OP_QUERY_SCAN.
	OpQueryScanCursorGetPage = 2001
	// OpResourceClose closes a resource, such as query cursor.
	OpResourceClose = 0
)
