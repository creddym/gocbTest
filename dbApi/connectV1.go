package dbApi

import (
	cfg "gocbtest/config"
	"gopkg.in/couchbase/gocb.v1"
	"log"
	"math/rand"
	"sync"
)

type CoucDbConnV1_t struct {
	Cluster *gocb.Cluster
	Bucket  *gocb.Bucket
	Mutex   *sync.Mutex
}

// these are safe to use concurrently acc to doc
var CoucDBConnListV1 map[int]CoucDbConnV1_t

func GetBucket() (*gocb.Bucket, int) {
	idx := rand.Intn(MaxCouchDbConn)
	return CoucDBConnListV1[idx].Bucket, idx
}

func ConnectToCBv1(url, bucketName, userName, passWd string) (CoucDbConnV1_t, error) {
	lCBConn := CoucDbConnV1_t{Cluster: nil, Bucket: nil, Mutex: &sync.Mutex{}}
	couchURL := url
	if cfg.GetEnableLog() {
		gocb.SetLogger(gocb.VerboseStdioLogger())
	}
	lCluster, err := gocb.Connect(couchURL)
	if err != nil || lCluster == nil {
		log.Print("Couchbase cluster object creation failed with err:", err)
		return lCBConn, err
	}

	err = lCluster.Authenticate(gocb.PasswordAuthenticator{
		Username: userName,
		Password: passWd,
	})

	lBucket, err := lCluster.OpenBucket(bucketName, "")
	lCBConn = CoucDbConnV1_t{Cluster: lCluster, Bucket: lBucket, Mutex: &sync.Mutex{}}
	return lCBConn, err
}

func CreateCouchDBConnectionsV1(couchDbCfg cfg.Db) int {
	url := couchDbCfg.URL
	bucketName := couchDbCfg.Bucket
	userName := couchDbCfg.User
	passWd := couchDbCfg.Passwd
	numOfConn := couchDbCfg.NumOfConn
	MaxCouchDbConn = numOfConn
	if MaxCouchDbConn <= 0 {
		MaxCouchDbConn = DEFAULT_MAX_COUCHDB_CONN
	}
	CoucDBConnListV1 = make(map[int]CoucDbConnV1_t, MaxCouchDbConn)

	connSucc := 0
	for i := 0; i < numOfConn; i++ {
		// Conntect to DB
		lCBConn, err := ConnectToCBv1(url, bucketName, userName, passWd)
		CoucDBConnListV1[i] = lCBConn
		if err != nil {
			log.Print("Couchbase connection ", i, " failed with err:", err)
			continue
		}
		log.Print("DB connection ", i, " success for couchbase: ", url)
		connSucc += 1
	}
	return connSucc
}

func CloseDbConnectionsV1() {
	for _, lConn := range CoucDBConnListV1 {
		lCluster := lConn.Cluster
		if lCluster != nil {
			lCluster.Close()
		}
	}
}
