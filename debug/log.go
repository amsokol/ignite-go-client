package debug

import (
	"log"
	"os"
)

// MemoryLeakLogger is used to log memory leak warnings
var MemoryLeakLogger = log.New(os.Stderr, "MEMORY LEAK ", log.LstdFlags)
