package debug

import (
	"log"
	"os"
)

// ResourceLeakLogger is used to log resource leak warnings
var ResourceLeakLogger = log.New(os.Stderr, "RESOURCE LEAK ", log.LstdFlags)
