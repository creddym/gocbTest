package dbApi

import (
	cfg "gocbtest/config"
	"log"
	"time"
)

// Default it creates 1 connection
const DEFAULT_MAX_COUCHDB_CONN = 1

// will be set  by application as required
var MaxCouchDbConn int

// For loggin elapsed time
func logElapsedTime(startTime time.Time, dbFunc, method string, key string) {
	elapsedTime := time.Since(startTime)
	//Max DB allowed delays to log db elapsed time as error log
	if elapsedTime.Milliseconds() > int64(cfg.GetTolelateLatencyMs()) {
		log.Print("Exceeded db elapsed time for ", dbFunc, " key= ", key, " DB elapsed time:", elapsedTime)
	} else {
		//log.Print("Elapsed time for ", dbFunc, " key= ", key, " DB elapsed time=", elapsedTime)

	}
}
