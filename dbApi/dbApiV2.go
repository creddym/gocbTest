package dbApi

import (
	"errors"
	gocbV2 "github.com/couchbase/gocb/v2"
	cc "gocbtest/common"
	"log"
	"strings"
	"time"
)

//Options global variable
var (
	GetOptions = &gocbV2.GetOptions{}
)

func GetDocV2(key string) (interface{}, gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		startTime := time.Now()
		result, err := collection.Get(key, GetOptions)
		logElapsedTime(startTime, "GetDocV2()", cc.DB_GET, key)
		if err == nil {
			var data interface{}
			if err = result.Content(&data); err == nil {
				return data, result.Cas(), nil
			} else {
				log.Print("GetDocV2 ", " idx=  ", idx, " key=  ", key, "DB Error:", err.Error())
				return nil, cc.ZERO, err
			}
		} else {
			log.Print("GetDocV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return nil, cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("GetDocV2", " idx= ", idx, " key= ", key, "DB Error:", err)
		return nil, 0, err
	}
}

func InsertDocV2(key string, value interface{}, expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		var insertOptions gocbV2.InsertOptions
		insertOptions.Expiry = expiry
		startTime := time.Now()
		mutateResult, err := collection.Insert(key, value, &insertOptions)
		logElapsedTime(startTime, "InsertDoc()", cc.INSERT, key)
		if err == nil {
			return mutateResult.Result.Cas(), err
		} else {
			log.Print("InsertDocV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		return cc.ZERO, err
	}
}

func UpsertDocV2(key string, value interface{}, expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		var upsertOptions gocbV2.UpsertOptions
		upsertOptions.Expiry = expiry
		startTime := time.Now()
		mutateResult, err := collection.Upsert(key, value, &upsertOptions)
		logElapsedTime(startTime, "UpsertDocV2()", cc.UPSERT, key)
		if err == nil {
			return mutateResult.Result.Cas(), err
		} else {
			log.Print("UpsertDocV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("UpsertDocV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}
}

func UpsertDocWithCasV2(key string, cas gocbV2.Cas, value interface{},
	expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		var replaceOptions gocbV2.ReplaceOptions
		replaceOptions.Expiry = expiry
		replaceOptions.Cas = cas
		startTime := time.Now()
		mutateResult, err := collection.Replace(key, value, &replaceOptions)
		logElapsedTime(startTime, "UpsertDocWithCas()", cc.REPLACE_OP, key)
		if err == nil {
			return mutateResult.Result.Cas(), err
		} else {
			log.Print("UpsertDocWithCasV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("UpsertDocWithCasV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}
}

func DeleteDocV2(key string, cas gocbV2.Cas) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		var removeOptions = &gocbV2.RemoveOptions{
			Cas: cas,
		}
		startTime := time.Now()
		mutateResult, err := collection.Remove(key, removeOptions)
		logElapsedTime(startTime, "DeleteDoc()", cc.DB_DELETE, key)
		if err == nil {
			return mutateResult.Result.Cas(), err
		} else {
			log.Print("DeleteDocV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("DeleteDocV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}
}

// Multi key DB APIs
type Result2 struct {
	Doc    interface{}
	Cas    gocbV2.Cas
	Expiry uint32
	Err    error
}

func GetMultiDocV2(keys []string) (map[string]Result2, error) {
	resp := make(map[string]Result2)
	if collection, idx := GetCollection(); collection != nil {
		ops := make([]gocbV2.BulkOp, len(keys))
		for idx, key := range keys {
			ops[idx] = &gocbV2.GetOp{ID: key}
		}
		startTime := time.Now()
		multiKey := strings.Join(keys[:], cc.COMMA)
		err := collection.Do(ops, nil)
		logElapsedTime(startTime, "GetMultiDocV2()", cc.DB_GET, multiKey)
		if err == nil {
			for _, op := range ops {
				if getOp := op.(*gocbV2.GetOp); getOp.Err == nil {
					doc := resp[getOp.ID].Doc
					getOp.Result.Content(&doc)
					resp[getOp.ID] = Result2{Doc: doc, Cas: getOp.Result.Cas()}
				} else {
					errs := strings.Split(getOp.Err.Error(), cc.PIPE)
					if len(errs) > cc.ZERO {
						opErr := errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
						resp[getOp.ID] = Result2{Err: opErr}
					}
				}
			}
			return resp, err
		} else {
			log.Print("GetMultiDocV2", " idx= ", idx, "keys=", keys, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return resp, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("GetMultiDocV2", " idx= ", idx, "keys=", keys, "DB Error:", err.Error())
		return resp, err
	}
}

func DeleteMultiDocV2(docs map[string]gocbV2.Cas) (map[string]Result2, error) {
	resp := make(map[string]Result2)
	if collection, idx := GetCollection(); collection != nil {
		keys := make([]string, len(docs))
		ops := make([]gocbV2.BulkOp, len(docs))
		idx := cc.ZERO
		for key, _ := range docs {
			ops[idx] = &gocbV2.RemoveOp{ID: key}
			keys[idx] = key
			idx += cc.ONE
		}

		startTime := time.Now()
		multiKey := strings.Join(keys[:], cc.COMMA)
		err := collection.Do(ops, nil)
		logElapsedTime(startTime, "DeleteMultiDocV2()", cc.DB_DELETE, multiKey)
		if err == nil {
			for _, op := range ops {
				removeOp := op.(*gocbV2.RemoveOp)
				if removeOp.Err == nil {
					resp[removeOp.ID] = Result2{Cas: removeOp.Result.Cas()}
				} else {
					errs := strings.Split(removeOp.Err.Error(), cc.PIPE)
					if len(errs) > cc.ZERO {
						opErr := errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
						resp[removeOp.ID] = Result2{Err: opErr}
					}
				}
			}
			return resp, err
		} else {
			log.Print("DeleteMultiDocV2", " idx= ", idx, "keys=", keys, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return resp, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("DeleteMultiDocV2", " idx= ", idx, "DB Error:", err.Error())
		return resp, err
	}
}

func InsertMultiDocV2(docs map[string]Result2) (map[string]Result2, error) {
	resp := make(map[string]Result2)
	if collection, idx := GetCollection(); collection != nil {
		keys := make([]string, len(docs))
		ops := make([]gocbV2.BulkOp, len(docs))
		idx := cc.ZERO
		for key, item := range docs {
			ops[idx] = &gocbV2.InsertOp{ID: key, Value: item.Doc}
			keys[idx] = key
			idx += cc.ONE
		}
		startTime := time.Now()
		multiKey := strings.Join(keys[:], cc.COMMA)
		err := collection.Do(ops, nil)
		logElapsedTime(startTime, "InsertMultiDocV2()", cc.BULK_INSERT, multiKey)
		if err == nil {
			for _, op := range ops {
				insertOp := op.(*gocbV2.InsertOp)
				if insertOp.Err == nil {
					resp[insertOp.ID] = Result2{Cas: insertOp.Result.Cas()}
				} else {
					errs := strings.Split(insertOp.Err.Error(), cc.PIPE)
					if len(errs) > cc.ZERO {
						opErr := errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
						resp[insertOp.ID] = Result2{Err: opErr}
					}
				}
			}
			return resp, err
		} else {
			log.Print("InsertMultiDocV2", " idx= ", idx, "keys=", keys, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return resp, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("InsertMultiDocV2", " idx= ", idx, "DB Error:", err.Error())
		return resp, err
	}
}

func UpsertMultiDocV2(docs map[string]Result2) (map[string]Result2, error) {
	resp := make(map[string]Result2)
	if collection, idx := GetCollection(); collection != nil {
		keys := make([]string, len(docs))
		ops := make([]gocbV2.BulkOp, len(docs))
		idx := cc.ZERO
		for key, item := range docs {
			ops[idx] = &gocbV2.UpsertOp{ID: key, Value: item.Doc}
			keys[idx] = key
			idx += cc.ONE
		}
		startTime := time.Now()
		multiKey := strings.Join(keys[:], cc.COMMA)
		err := collection.Do(ops, nil)
		logElapsedTime(startTime, "UpsertMultiDocV2()", cc.BULK_UPSERT, multiKey)
		if err == nil {
			for _, op := range ops {
				upsertOp := op.(*gocbV2.UpsertOp)
				if upsertOp.Err == nil {
					resp[upsertOp.ID] = Result2{Cas: upsertOp.Result.Cas()}
				} else {
					errs := strings.Split(upsertOp.Err.Error(), cc.PIPE)
					if len(errs) > cc.ZERO {
						opErr := errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
						resp[upsertOp.ID] = Result2{Err: opErr}
					}
				}
			}
			return resp, err
		} else {
			log.Print("UpsertMultiDocV2", " idx= ", idx, "keys=", keys, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return resp, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("UpsertMultiDocV2", " idx= ", idx, "DB Error:", err.Error())
		return resp, err
	}
}

// PATCH DB APIs
func PatchDocAddV2(key string, path string, value interface{}, cas gocbV2.Cas,
	expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		mutateInoptions := &gocbV2.MutateInOptions{
			Expiry: expiry,
			Cas:    cas,
		}
		startTime := time.Now()
		subRes, err := collection.MutateIn(key, []gocbV2.MutateInSpec{
			gocbV2.InsertSpec(path, value, nil)}, mutateInoptions)
		logElapsedTime(startTime, "PatchDocAddV2()", cc.DB_PATCH, key)
		if err == nil {
			return subRes.Cas(), err
		} else {
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("PatchDocAddV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}
}

func PatchDocAddArrayV2(key string, path string, value interface{}, cas gocbV2.Cas,
	expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		var mutateInoptions gocbV2.MutateInOptions
		mutateInoptions.Expiry = expiry
		mutateInoptions.Cas = cas
		startTime := time.Now()
		subRes, err := collection.MutateIn(key, []gocbV2.MutateInSpec{
			gocbV2.ArrayInsertSpec(path, value, nil)}, &mutateInoptions)
		logElapsedTime(startTime, "PatchDocAddArrayV2()", cc.DB_PATCH, key)
		if err == nil {
			return subRes.Cas(), err
		} else {
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("PatchDocAddArrayV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}

}

func PatchDocReplaceV2(key string, path string, value interface{}, cas gocbV2.Cas,
	expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		mutateInoptions := &gocbV2.MutateInOptions{
			Expiry: expiry,
			Cas:    cas,
		}
		startTime := time.Now()
		subRes, err := collection.MutateIn(key, []gocbV2.MutateInSpec{
			gocbV2.ReplaceSpec(path, value, nil)}, mutateInoptions)
		logElapsedTime(startTime, "PatchDocReplaceV2()", cc.DB_PATCH, key)
		if err == nil {
			return subRes.Cas(), err
		} else {
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("PatchDocAddArrayV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}

}

func PatchDocUpsertV2(key string, path string, value interface{}, cas gocbV2.Cas,
	expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		mutateInoptions := &gocbV2.MutateInOptions{
			Expiry: expiry,
			Cas:    cas,
		}
		startTime := time.Now()
		subRes, err := collection.MutateIn(key, []gocbV2.MutateInSpec{
			gocbV2.UpsertSpec(path, value, nil)}, mutateInoptions)
		logElapsedTime(startTime, "PatchDocUpsertV2()", cc.DB_PATCH, key)
		if err == nil {
			return subRes.Cas(), err
		} else {
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("PatchDocUpsertV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}

}

func PatchDocRemoveV2(key string, path string, cas gocbV2.Cas,
	expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		mutateInoptions := &gocbV2.MutateInOptions{
			Expiry: expiry,
			Cas:    cas,
		}
		startTime := time.Now()
		subRes, err := collection.MutateIn(key, []gocbV2.MutateInSpec{
			gocbV2.RemoveSpec(path, nil)}, mutateInoptions)
		logElapsedTime(startTime, "PatchDocRemoveV2()", cc.DB_PATCH, key)
		if err == nil {
			return subRes.Cas(), err
		} else {
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("PatchDocRemoveV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}

}

func PatchDocTestV2(key string, path string, value interface{}) (bool, error) {
	if collection, idx := GetCollection(); collection != nil {
		startTime := time.Now()
		subRes, err := collection.LookupIn(key, []gocbV2.LookupInSpec{
			gocbV2.GetSpec(path, nil)}, nil)
		logElapsedTime(startTime, "PatchDocTestV2()", cc.DB_PATCH, key)
		if err == nil {
			var data interface{}
			if err = subRes.ContentAt(cc.ZERO, &data); err == nil {
				if value != data {
					log.Print("PatchDocTestV2", " idx= ", idx, " key= ", key, "DB Error: value not found -", value)
					err = errors.New("Value not found")
					return false, err
				}
				return true, err
			} else {
				log.Print("PatchDocTestV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
				errs := strings.Split(err.Error(), cc.PIPE)
				if len(errs) > cc.ZERO {
					err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
				}
				return false, err
			}
		} else {
			log.Print("PatchDocTestV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return false, err
		}

	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("PatchDocTestV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return false, err
	}
}

func PatchDocCopyV2(key string, frompath string, topath string, cas gocbV2.Cas,
	expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		startTime := time.Now()
		subRes, err := collection.LookupIn(key, []gocbV2.LookupInSpec{
			gocbV2.GetSpec(frompath, nil)}, nil)
		logElapsedTime(startTime, "PatchDocCopyV2()", cc.DB_PATCH, key)
		var data interface{}
		if err = subRes.ContentAt(cc.ZERO, &data); err == nil {
			var mutateInoptions gocbV2.MutateInOptions
			mutateInoptions.Expiry = expiry
			mutateInoptions.Cas = cas
			startTime = time.Now()
			res, err := collection.MutateIn(key, []gocbV2.MutateInSpec{
				gocbV2.UpsertSpec(topath, data, nil)}, &mutateInoptions)
			logElapsedTime(startTime, "PatchDocCopyV2()", cc.DB_PATCH, key)
			if err != nil {
				errs := strings.Split(err.Error(), cc.PIPE)
				if len(errs) > cc.ZERO {
					err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
				}
				return cc.ZERO, err
			}
			return res.Cas(), err
		} else {
			log.Print("PatchDocCopyV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("PatchDocCopyV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}
}

func PatchDocMoveV2(key string, frompath string, topath string, cas gocbV2.Cas,
	expiry time.Duration) (gocbV2.Cas, error) {
	if collection, idx := GetCollection(); collection != nil {
		startTime := time.Now()
		subRes, err := collection.LookupIn(key, []gocbV2.LookupInSpec{
			gocbV2.GetSpec(frompath, nil)}, nil)
		logElapsedTime(startTime, "PatchDocMoveV2()", cc.DB_PATCH, key)
		var data interface{}
		if err = subRes.ContentAt(cc.ZERO, &data); err == nil {
			var mutateInoptions gocbV2.MutateInOptions
			mutateInoptions.Expiry = expiry
			mutateInoptions.Cas = cas
			startTime = time.Now()
			res, err := collection.MutateIn(key, []gocbV2.MutateInSpec{gocbV2.RemoveSpec(frompath, nil),
				gocbV2.UpsertSpec(topath, data, nil)}, &mutateInoptions)
			logElapsedTime(startTime, "PatchDocMoveV2()", cc.DB_PATCH, key)
			if err == nil {
				return res.Cas(), err
			} else {
				errs := strings.Split(err.Error(), cc.PIPE)
				if len(errs) > cc.ZERO {
					err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
				}
				return cc.ZERO, err
			}
		} else {
			log.Print("PatchDocMoveV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
			errs := strings.Split(err.Error(), cc.PIPE)
			if len(errs) > cc.ZERO {
				err = errors.New(strings.TrimRight(errs[cc.ZERO], cc.SPACE))
			}
			return cc.ZERO, err
		}
	} else {
		err := errors.New(cc.CONNECTION_SHUT_DOWN)
		log.Print("PatchDocMoveV2", " idx= ", idx, " key= ", key, "DB Error:", err.Error())
		return cc.ZERO, err
	}
}

func logElapsedTime(startTime time.Time, dbFunc, method string, key string) {
	elapsedTime := time.Since(startTime)
	//Max DB allowed delays to log db elapsed time as error log
	if elapsedTime.Milliseconds() > int64(CouchDbCfg.TolelateLatencyMs) {
		log.Print("Exceeded db elapsed time for ", dbFunc, " key= ", key, " DB elapsed time:", elapsedTime)
	} else {
		//log.Print("Elapsed time for ", dbFunc, " key= ", key, " DB elapsed time=", elapsedTime)

	}
}
