package appApi

import (
	"encoding/json"
	"github.com/gorilla/mux"
	cc "gocbtest/common"
	dbApi "gocbtest/dbApi"
	"net/http"
)

func GetData(w http.ResponseWriter, r *http.Request) {
	//ruri := r.URL.RequestURI()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	params := mux.Vars(r)
	key := params[cc.KEY]
	dbKey := cc.KEY_PREFIX + cc.KEYSEP + key
	data, _, err := dbApi.GetDocV2(dbKey)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var doc []byte
	doc, err = json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(doc)
	//w.WriteHeader(http.StatusOK)
}
