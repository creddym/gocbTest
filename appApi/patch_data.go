package appApi

import (
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	cc "gocbtest/common"
	dbApi "gocbtest/dbApi"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

type EnumPatchOperation string

var (
	Add     EnumPatchOperation = "add"
	Copy    EnumPatchOperation = "copy"
	Move    EnumPatchOperation = "move"
	Remove  EnumPatchOperation = "remove"
	Replace EnumPatchOperation = "replace"
	Test    EnumPatchOperation = "test"
)

type PatchItem struct {
	Op    EnumPatchOperation `json:"op"`
	Path  string             `json:"path"`
	From  string             `json:"from,omitempty"`
	Value interface{}        `json:"value,omitempty"`
}

type PatchItemArray struct {
	PatchItem []PatchItem `json:"PatchItem"`
}

type PatchItemReqInfo struct {
	Key       string
	Expiry    time.Duration
	PatchItem PatchItem
}

func PatchData(w http.ResponseWriter, r *http.Request) {
	//ruri := r.URL.RequestURI()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	params := mux.Vars(r)
	key := params[cc.KEY]
	doc, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	//Get the PatchItem Array from the request paylod
	patchItemArray := PatchItemArray{}
	err = json.Unmarshal(doc, &patchItemArray.PatchItem)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	dbKey := cc.KEY_PREFIX + cc.KEYSEP + key
	patchData := PatchItemReqInfo{Key: dbKey, Expiry: 0}
	for i := 0; i < len(patchItemArray.PatchItem); i++ {
		patchData.PatchItem = patchItemArray.PatchItem[i]
		err := PatchDocWithData(&patchData)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	//w.WriteHeader(http.StatusOK)
}

func PatchDocWithData(patchData *PatchItemReqInfo) error {
	key := patchData.Key
	expiry := patchData.Expiry
	patchOperation := patchData.PatchItem.Op
	path := patchData.PatchItem.Path
	value := patchData.PatchItem.Value
	fromPath := patchData.PatchItem.From
	if path != "" && path[0:1] == cc.FSLASH {
		// first "/" of path is ignored as DB API is expecting without first "."
		dbPath, ok := GetDBPatchPath(path[1:])
		if !ok {
			err := errors.New("Invaild Patch Path")
			return err
		}
		path = dbPath
	}
	if fromPath != "" && fromPath[0:1] == cc.FSLASH {
		// first "/" of path is ignored as DB API is expecting without first "."
		dbFromPath, ok := GetDBPatchPath(fromPath[1:])
		if !ok {
			err := errors.New("Invaild Patch Path")
			return err
		}
		fromPath = dbFromPath
	}

	switch patchOperation {
	case "add":
		lastTwoCharOfPath := ""
		if len(path) > 2 {
			lastTwoCharOfPath = path[len(path)-2 : len(path)]
		}
		// If last two character of path is "]." then path is an array insert
		// eg: "path":"VolumeAccounting.value[0]." - insert into zero position

		if lastTwoCharOfPath == cc.CLOSEARRAY+cc.DOT {
			_, err := dbApi.PatchDocAddArrayV2(key, path, value, 0, expiry)
			if err != nil {
				return err
			}

		} else {
			_, err := dbApi.PatchDocAddV2(key, path, value, 0, expiry)
			if err != nil {
				return err
			}
		}
	case "copy":
		_, err := dbApi.PatchDocCopyV2(key, fromPath, path, 0, expiry)
		if err != nil {
			return err
		}
	case "move":
		_, err := dbApi.PatchDocMoveV2(key, fromPath, path, 0, expiry)
		if err != nil {
			return err
		}

	case "remove":
		_, err := dbApi.PatchDocRemoveV2(key, path, 0, expiry)
		if err != nil {
			return err
		}

	case "replace":
		_, err := dbApi.PatchDocReplaceV2(key, path, value, 0, expiry)
		if err != nil {
			return err
		}
	case "test":
		_, err := dbApi.PatchDocTestV2(key, path, value)
		if err != nil {
			return err
		}
	default:
		log.Print("Not a Patch operation=", patchOperation)
		err := errors.New("Invaild Patch Operation")
		return err
	}
	return nil
}

/* This function convets jpath to couchbase patch path format
eg : input: a/0/b/1/c/d
     output : a[0].b[1].c.d
*/
func GetDBPatchPath(jpath string) (string, bool) {
	parts := strings.Split(jpath, cc.FSLASH)
	for _, val := range parts {
		if ok := strings.Contains(val, cc.DOT); ok {
			newVal := string(cc.ACUTE) + val + string(cc.ACUTE)
			jpath = strings.ReplaceAll(jpath, val, newVal)
		}
	}
	if !strings.HasSuffix(jpath, cc.FSLASH) {
		jpath = jpath + cc.FSLASH
	}
	regEx, ok := cc.CompiledRegExMap[cc.JSON_PATH]
	if !ok {
		return jpath, false
	}
	arryDot := regEx.FindAllString(jpath, -1)
	arrayPhrase := make([]string, len(arryDot))
	for k, v := range arryDot {
		arrayPhrase[k] = v
	}
	for k, v := range arryDot {
		arrayPhrase[k] = v
	}
	for k, v := range arryDot {
		v1 := strings.Replace(v, cc.FSLASH, cc.OPENARRAY, 1)
		v2 := strings.Replace(v1, cc.FSLASH, cc.CLOSEARRAY+cc.DOT, 1)
		arryDot[k] = v2
	}
	for i := 0; i < len(arryDot); i++ {
		jpath = strings.Replace(jpath, string(arrayPhrase[i]), string(arryDot[i]), -1)
	}
	jpath = strings.Replace(jpath, cc.FSLASH, cc.DOT, -1)
	return jpath, true
}
