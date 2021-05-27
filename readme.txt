This is to compare performance difference b/w gocb 2.2.3 and gocb 1.6.

1. cd dbTestMgr
2. go build
3. configure application (ports) and DB details
-bash-4.2$ cat dbTestMgr/appconfigV1.json 
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
4. Run dbTestMgr
./dbTestMgr -config appconfigV1.json 
Example:
2021/05/25 08:53:24 Exceeded db elapsed time for UpsertDocV2() key= TEST::405050000000018 DB elapsed time:122.97243ms
2021/05/25 08:53:24 Exceeded db elapsed time for UpsertDocV2() key= TEST::405050000000036 DB elapsed time:131.449096ms
2021/05/25 08:53:24 Exceeded db elapsed time for UpsertDocV2() key= TEST::405050000000038 DB elapsed time:131.692597ms
5. set gocbSdkVersion to v1 or v2 for selecting gocb v1.6.2 or gocb 2.2.3
6. set enableLog to true for capturing go SDK logs
7. set tolelateLatencyMs to accepted delay for DB calls
