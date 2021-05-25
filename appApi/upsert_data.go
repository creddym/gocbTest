package appApi

import (
	"encoding/json"
	"github.com/gorilla/mux"
	cc "gocbtest/common"
	dbApi "gocbtest/dbApi"
	"io/ioutil"
	"net/http"
)

func UpsertData(w http.ResponseWriter, r *http.Request) {
	//ruri := r.URL.RequestURI()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	params := mux.Vars(r)
	key := params[cc.KEY]
	doc, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	data := make(map[string]interface{})
	if err = json.Unmarshal(doc, &data); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	dbKey := cc.KEY_PREFIX + cc.KEYSEP + key
	_, err = dbApi.UpsertDocV2(dbKey, data, 0)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	//w.WriteHeader(http.StatusOK)
}
