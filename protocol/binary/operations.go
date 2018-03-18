package binary

const (
	// Cache Configuration
	opCacheGetNames                     = 1050
	opCacheCreateWithName               = 1051
	opCacheGetOrCreateWithName          = 1052
	opCacheCreateWithConfiguration      = 1053
	opCacheGetOrCreateWithConfiguration = 1054
	opCacheGetConfiguration             = 1055
	opCacheDestroy                      = 1056
)

const (
	// StatusSuccess means success
	StatusSuccess = 0
)

// Result is the operation execution result
type Result struct {
	// Status code (0 for success, otherwise error code)
	Status int
	// Error message (present only when status is not 0)
	Message string
}
