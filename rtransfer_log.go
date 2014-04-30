package rtransfer

import (
	"log"
)

var loggingEnabled = false

func SetLogging(enabled bool) {
	loggingEnabled = enabled
}

func logln(params ...interface{}) {
	if loggingEnabled {
		log.Println(params...)
	}
}

func logf(format string, params ...interface{}) {
	if loggingEnabled {
		log.Printf(format, params...)
	}
}
