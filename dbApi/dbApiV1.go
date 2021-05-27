package dbApi

import (
	"errors"
	cc "gocbtest/common"
	"gopkg.in/couchbase/gocb.v1"
	"log"
	"strings"
	"time"
)

func GetDocV1(key string) (interface{}, gocb.Cas, error) {
	var data interface{}
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("GetDocV1 ", " idx=  ", idx, " key=  ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return data, 0, err
	}
	startTime := time.Now()
	cas, err := bucket.Get(key, &data)
	logElapsedTime(startTime, "GetDocV1()", cc.DB_GET, key)
	return data, cas, err
}

func InsertDocV1(key string, value interface{}, expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("InsertDocV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		return 0, err
	}
	startTime := time.Now()
	cas, err := bucket.Insert(key, value, expiry)
	logElapsedTime(startTime, "InsertDocV1()", cc.INSERT, key)
	return cas, err
}

func UpsertDocV1(key string, value interface{}, expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("UpsertDocV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		return 0, err
	}
	startTime := time.Now()
	cas, err := bucket.Upsert(key, value, expiry)
	logElapsedTime(startTime, "UpsertDocV1()", cc.UPSERT, key)
	return cas, err
}

func UpsertDocWithCasV1(key string, cas gocb.Cas, value interface{},
	expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("UpsertDocWithCasV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		return 0, err
	}
	startTime := time.Now()
	cas, err := bucket.Replace(key, value, cas, expiry)
	logElapsedTime(startTime, "UpsertDocWithCasV1()", cc.REPLACE_OP, key)
	return cas, err
}

func DeleteDocV1(key string, cas gocb.Cas) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("DeleteDocV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		return 0, err
	}
	startTime := time.Now()
	cas, err := bucket.Remove(key, cas)
	logElapsedTime(startTime, "DeleteDocV1()", cc.DB_DELETE, key)
	return cas, err
}

// Multi key DB APIs
type ResultV1 struct {
	Doc    interface{}
	Cas    gocb.Cas
	Expiry uint32
	Err    error
}

func GetMultiDocV1(keys []string) (map[string]ResultV1, error) {
	result := make(map[string]ResultV1)
	bucket, idx := GetBucket()
	if bucket == nil {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("GetMultiDocV1", " idx= ", idx, " keys= ", keys, "DB Error: bucket=nil")
		return result, err
	}
	var ops []gocb.BulkOp

	for _, key := range keys {
		var data interface{}
		ops = append(ops, &gocb.GetOp{Key: key, Value: &data})
	}
	startTime := time.Now()
	err := bucket.Do(ops)
	multiKey := strings.Join(keys[:], cc.COMMA)
	logElapsedTime(startTime, "GetMultiDocV1()", cc.DB_GET, multiKey)

	for i := 0; i < len(ops); i++ {
		var res ResultV1
		op := ops[i].(*gocb.GetOp)

		if op.Err == nil {
			res.Doc = op.Value
			res.Cas = op.Cas
			res.Err = op.Err
			result[op.Key] = res
		} else {
			res.Err = op.Err
			result[op.Key] = res
		}
	}
	return result, err
}

func DeleteMultiDocV1(docs map[string]gocb.Cas) (map[string]ResultV1, error) {
	result := make(map[string]ResultV1)
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("DeleteMultiDocV1", " idx= ", idx, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return result, err
	}

	var keys []string
	var ops []gocb.BulkOp
	for key, item := range docs {
		ops = append(ops, &gocb.RemoveOp{Key: key, Cas: item})
		keys = append(keys, key)
	}

	startTime := time.Now()
	err := bucket.Do(ops)
	multiKey := strings.Join(keys[:], cc.COMMA)
	logElapsedTime(startTime, "DeleteMultiDocV1()", cc.DB_DELETE, multiKey)

	for i := 0; i < len(ops); i++ {
		var res ResultV1
		op := ops[i].(*gocb.RemoveOp)
		res.Cas = op.Cas
		res.Err = op.Err
		result[op.Key] = res
	}
	return result, err
}

func InsertMultiDocV1(docs map[string]ResultV1) (map[string]ResultV1, error) {
	result := make(map[string]ResultV1)
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("InsertMultiDocV1", " idx= ", idx, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return result, err
	}

	var keys []string
	var ops []gocb.BulkOp
	for key, item := range docs {
		ops = append(ops, &gocb.InsertOp{Key: key, Value: item.Doc, Expiry: item.Expiry})
		keys = append(keys, key)
	}

	startTime := time.Now()
	err := bucket.Do(ops)
	multiKey := strings.Join(keys[:], cc.COMMA)
	logElapsedTime(startTime, "InsertMultiDoc()", cc.INSERT, multiKey)

	for i := 0; i < len(ops); i++ {
		var res ResultV1
		op := ops[i].(*gocb.InsertOp)
		res.Cas = op.Cas
		res.Err = op.Err
		result[op.Key] = res
	}
	return result, err
}

func UpsertMultiDocV1(docs map[string]ResultV1) (map[string]ResultV1, error) {
	result := make(map[string]ResultV1)
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("UpsertMultiDocV1", " idx= ", idx, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return result, err
	}
	var keys []string
	var ops []gocb.BulkOp
	for key, item := range docs {
		ops = append(ops, &gocb.UpsertOp{Key: key, Value: item.Doc, Expiry: item.Expiry})
		keys = append(keys, key)
	}

	startTime := time.Now()
	err := bucket.Do(ops)
	multiKey := strings.Join(keys[:], cc.COMMA)
	logElapsedTime(startTime, "UpsertMultiDocV1()", cc.UPSERT, multiKey)

	for i := 0; i < len(ops); i++ {
		var res ResultV1
		op := ops[i].(*gocb.UpsertOp)
		res.Cas = op.Cas
		res.Err = op.Err
		result[op.Key] = res
	}
	return result, err
}

// PATCH DB APIs
func PatchDocAddV1(key string, path string, value interface{}, cas gocb.Cas,
	expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("PatchDocAddV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return 0, err
	}
	startTime := time.Now()
	docFragment, err := bucket.MutateIn(key, cas, expiry).Insert(path, value, false).Execute()
	logElapsedTime(startTime, "PatchDocAddV1()", cc.DB_PATCH, key)
	return docFragment.Cas(), err
}

func PatchDocAddArrayV1(key string, path string, value interface{}, cas gocb.Cas,
	expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("PatchDocAddArrayV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return 0, err
	}
	startTime := time.Now()
	docFragment, err := bucket.MutateIn(key, cas, expiry).Insert(path, value, false).Execute()
	logElapsedTime(startTime, "PatchDocAddArrayV1()", cc.DB_PATCH, key)
	return docFragment.Cas(), err
}

func PatchDocReplaceV1(key string, path string, value interface{}, cas gocb.Cas,
	expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("PatchDocReplaceV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return 0, err
	}
	startTime := time.Now()
	docFragment, err := bucket.MutateIn(key, cas, expiry).Replace(path, value).Execute()
	logElapsedTime(startTime, "PatchDocReplaceV1()", cc.DB_PATCH, key)
	return docFragment.Cas(), err
}

func PatchDocUpsertV1(key string, path string, value interface{}, cas gocb.Cas,
	expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("PatchDocUpsertV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return 0, err
	}
	startTime := time.Now()
	docFragment, err := bucket.MutateIn(key, cas, expiry).Upsert(path, value, false).Execute()
	logElapsedTime(startTime, "PatchDocUpsertV1()", cc.DB_PATCH, key)
	return docFragment.Cas(), err
}

func PatchDocRemoveV1(key string, path string, cas gocb.Cas,
	expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("PatchDocRemoveV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return 0, err
	}

	startTime := time.Now()
	docFragment, err := bucket.MutateIn(key, cas, expiry).Remove(path).Execute()
	logElapsedTime(startTime, "PatchDocRemoveV1()", cc.DB_PATCH, key)
	return docFragment.Cas(), err
}

func PatchDocTestV1(key string, path string, value interface{}) (bool, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("PatchDocTestV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return false, err
	}
	startTime := time.Now()
	docFragment, err := bucket.LookupIn(key).Get(path).Execute()
	logElapsedTime(startTime, "PatchDocTestV1()", cc.DB_PATCH, key)
	var data interface{}

	err = docFragment.Content(path, &data)

	if value != data {
		log.Print("PatchDocTestV1", " idx= ", idx, " key= ", key, "DB Error: value not found -", value)
		return false, err
	}
	return true, err
}

func PatchDocCopyV1(key string, frompath string, topath string, cas gocb.Cas,
	expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("PatchDocCopyV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return 0, err
	}
	startTime := time.Now()
	docFragment, err := bucket.LookupIn(key).Get(frompath).Execute()
	logElapsedTime(startTime, "PatchDocCopyV1()", cc.DB_GET, key)
	var data interface{}

	err = docFragment.Content(frompath, &data)
	if err != nil {
		log.Print("PatchDocCopyV1", " idx= ", idx, " key= ", key, "DB Error: ", err.Error())
		return 0, err
	}

	startTime = time.Now()
	docFragment, err = bucket.MutateIn(key, cas, expiry).Upsert(topath, data, false).Execute()
	logElapsedTime(startTime, "PatchDocCopyV1()", cc.DB_PATCH, key)
	return docFragment.Cas(), err
}

func PatchDocMoveV1(key string, frompath string, topath string, cas gocb.Cas,
	expiry uint32) (gocb.Cas, error) {
	bucket, idx := GetBucket()
	if bucket == nil {
		log.Print("PatchDocMoveV1", " idx= ", idx, " key= ", key, "DB Error: bucket=nil")
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return 0, err
	}
	startTime := time.Now()
	docFragment, err := bucket.LookupIn(key).Get(frompath).Execute()
	logElapsedTime(startTime, "PatchDocMoveV1()", cc.DB_GET, key)
	var data interface{}

	err = docFragment.Content(frompath, &data)
	if err != nil {
		log.Print("PatchDocMoveV1", " idx= ", idx, " key= ", key, "DB Error: ", err.Error())
		return 0, err
	}

	startTime = time.Now()
	docFragment, err = bucket.MutateIn(key, cas, expiry).Remove(frompath).Upsert(topath, data, false).Execute()
	logElapsedTime(startTime, "PatchDocMoveV1()", cc.DB_PATCH, key)
	return docFragment.Cas(), err
}
