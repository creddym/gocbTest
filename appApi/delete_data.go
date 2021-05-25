package appApi

import (
	"github.com/gorilla/mux"
	cc "gocbtest/common"
	dbApi "gocbtest/dbApi"
	"net/http"
)

func DeleteData(w http.ResponseWriter, r *http.Request) {
	//ruri := r.URL.RequestURI()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	params := mux.Vars(r)
	key := params[cc.KEY]
	dbKey := cc.KEY_PREFIX + cc.KEYSEP + key
	_, err := dbApi.DeleteDocV2(dbKey, 0)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	//w.WriteHeader(http.StatusOK)
}
