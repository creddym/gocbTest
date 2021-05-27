package dbApi

import (
	cfg "gocbtest/config"
)

func CreateCouchDBConnections(couchDbCfg cfg.Db) int {
	connected := 0
	switch cfg.GetGocbSdkVersion() {
	case "v2":
		connected = CreateCouchDBConnectionsV2(couchDbCfg)
	default:
		connected = CreateCouchDBConnectionsV1(couchDbCfg)
	}
	return connected
}

func CloseDbConnections() {
	switch cfg.GetGocbSdkVersion() {
	case "v2":
		CloseDbConnectionsV2()
	default:
		CloseDbConnectionsV1()
	}
}
