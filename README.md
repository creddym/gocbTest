# gocbTest
testing gocb 2.2.3

cd dbTestMgr

go build

First instance with gocb v1

configure SDK version and DB details (edit appConfigV1.json as required)

-bash-4.2$ cat appConfigV1.json

{

   "app":{
   
      "port":8080,
      
      "profilePort":6060,
      
      "logLevel":"DEBUG"
      
   },
   
   "db":{
   
      "url":"couchbase://crdl.host",
      
      "bucket":"db",
      
      "user":"user",
      
      "passwd":"passwd",
      
      "numOfConn":8,
      
      "tolelateLatencyMs":100,
      
      "enableLog":false,
      
      "gocbSdkVersion":"v1"
      
   }
   
}

Run dbMgr

-bash-4.2$ ./dbTestMgr -config appConfigV1.json

Second instance with gocb v2

configure SDK version and DB details (edit appConfigV1.json as required)

-bash-4.2$ cat appConfigV2.json

{
   "app":{
   
      "port":8081,
      
      "profilePort":6061,
      
      "logLevel":"DEBUG"
      
   },
   
   "db":{
   
      "url":"couchbase://crdl.host",
      
      "bucket":"db",
      
      "user":"user",
      
      "passwd":"passwd",
      
      "numOfConn":8,
      
      "tolelateLatencyMs":100,
      
      "enableLog":false,
      
      "gocbSdkVersion":"v2"
   }
   
}

Run dbMgr

-bash-4.2$ ./dbTestMgr -config appConfigV2.json

Logs:

2021/05/25 08:52:19 DB connection 0 success for couchbase: couchbase://crdl.svc/persistdb_test

2021/05/25 08:52:20 DB connection 1 success for couchbase: couchbase://crdl.svc/persistdb_test

2021/05/25 08:52:21 DB connection 2 success for couchbase: couchbase://crdl.svc/persistdb_test_

2021/05/25 08:52:35 Running server..., port= :8080

If there are exceeded latency logs, then it is an issue.

Example:

2021/05/25 08:53:24 Exceeded db elapsed time for UpsertDocV2() key= TEST::405050000000018 DB elapsed time:122.97243ms

2021/05/25 08:53:24 Exceeded db elapsed time for UpsertDocV2() key= TEST::405050000000036 DB elapsed time:131.449096ms

2021/05/25 08:53:24 Exceeded db elapsed time for UpsertDocV2() key= TEST::405050000000038 DB elapsed time:131.692597ms
