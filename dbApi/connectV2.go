package dbApi

import (
	"github.com/couchbase/gocb/v2"
	cfg "gocbtest/config"
	"log"
	"math/rand"
	"sync"
	"time"
)

type CoucDbConnV2_t struct {
	Cluster    *gocb.Cluster
	Collection *gocb.Collection
	Mutex      *sync.Mutex
}

// these are safe to use concurrently acc to doc
var CoucDBConnListV2 map[int]CoucDbConnV2_t

func GetCollection() (*gocb.Collection, int) {
	idx := rand.Intn(MaxCouchDbConn)
	return CoucDBConnListV2[idx].Collection, idx
}

func ConnectToCBv2(url, bucketName, userName, passWd string) (CoucDbConnV2_t, error) {
	lCBConn := CoucDbConnV2_t{Cluster: nil, Collection: nil, Mutex: &sync.Mutex{}}
	couchURL := url
	if cfg.GetEnableLog() {
		gocb.SetLogger(gocb.VerboseStdioLogger())
	}
	clusterOptions := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: userName,
			Password: passWd,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout: 60000 * time.Millisecond,
			KVTimeout:      2500 * time.Millisecond,
		},
		OrphanReporterConfig: gocb.OrphanReporterConfig{
			Disabled: true,
		},
	}
	lCluster, err := gocb.Connect(couchURL, clusterOptions)
	if err != nil || lCluster == nil {
		log.Print("Couchbase cluster object creation failed with err:", err)
		return lCBConn, err
		//} else if err = lCluster.WaitUntilReady(60*time.Second, nil); err != nil {
		//	log.Print("Cluster is not ready. Error:", err)
	}

	lBucket := lCluster.Bucket(bucketName)
	if err = lBucket.WaitUntilReady(60*time.Second, nil); err != nil {
		log.Print("Bucket is not ready. Error:", err)
	}
	lCollection := lBucket.DefaultCollection()
	lCBConn = CoucDbConnV2_t{Cluster: lCluster, Collection: lCollection, Mutex: &sync.Mutex{}}
	return lCBConn, err
}

func CreateCouchDBConnectionsV2(couchDbCfg cfg.Db) int {
	url := couchDbCfg.URL
	bucketName := couchDbCfg.Bucket
	userName := couchDbCfg.User
	passWd := couchDbCfg.Passwd
	numOfConn := couchDbCfg.NumOfConn
	MaxCouchDbConn = numOfConn
	if MaxCouchDbConn <= 0 {
		MaxCouchDbConn = DEFAULT_MAX_COUCHDB_CONN
	}
	CoucDBConnListV2 = make(map[int]CoucDbConnV2_t, MaxCouchDbConn)

	connSucc := 0
	for i := 0; i < numOfConn; i++ {
		// Conntect to DB
		lCBConn, err := ConnectToCBv2(url, bucketName, userName, passWd)
		CoucDBConnListV2[i] = lCBConn
		if err != nil {
			log.Print("Couchbase connection ", i, " failed with err:", err)
			continue
		}
		log.Print("DB connection ", i, " success for couchbase: ", url)
		connSucc += 1
	}
	return connSucc
}

func CloseDbConnectionsV2() {
	for _, lConn := range CoucDBConnListV2 {
		lCluster := lConn.Cluster
		if lCluster != nil {
			clusterCloseOpts := gocb.ClusterCloseOptions{}
			lCluster.Close(&clusterCloseOpts)
		}
	}
}
