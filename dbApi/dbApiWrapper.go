package dbApi

import (
	"github.com/couchbase/gocb/v2"
	cfg "gocbtest/config"
	//gocV1 "gopkg.in/couchbase/gocb.v1"
	"time"
)

func GetDoc(key string) (interface{}, gocb.Cas, error) {
	switch cfg.GetGocbSdkVersion() {
	case "v2":
		return GetDocV2(key)
	default:
		data, cas, err := GetDocV1(key)
		return data, (gocb.Cas)(cas), err
	}
}

func UpsertDoc(key string, value interface{}, expiry time.Duration) (gocb.Cas, error) {
	switch cfg.GetGocbSdkVersion() {
	case "v2":
		return UpsertDocV2(key, value, expiry)
	default:
		cas, err := UpsertDocV1(key, value, (uint32)(expiry))
		return (gocb.Cas)(cas), err
	}
}

func InsertDoc(key string, value interface{}, expiry time.Duration) (gocb.Cas, error) {
	switch cfg.GetGocbSdkVersion() {
	case "v2":
		return InsertDocV2(key, value, expiry)
	default:
		cas, err := InsertDocV1(key, value, (uint32)(expiry))
		return (gocb.Cas)(cas), err
	}
}

func DeleteDoc(key string, cas gocb.Cas) (gocb.Cas, error) {
	switch cfg.GetGocbSdkVersion() {
	case "v2":
		return DeleteDocV2(key, cas)
	default:
		//return DeleteDocV1(key, (gocbV1.Cas)(cas))
		return DeleteDocV2(key, cas)
	}
}
