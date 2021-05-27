package main

import (
	"encoding/json"
	"fmt"
	appApi "gocbtest/appApi"
	cc "gocbtest/common"
	cfg "gocbtest/config"
	dbApi "gocbtest/dbApi"
	"io/ioutil"
	//"github.com/gorilla/mux"
	"flag"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
)

func init() {
	//generates compiled regular expression
	// It is used in PATCH APIs
	GenerateRegExpMap(cc.RegExTypeMap)
}

func main() {

	cfgFilePath := flag.String("config", "", "application configuration file")
	flag.Parse()

	errs := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errs <- fmt.Errorf("%s", <-c)
	}()

	//Read DB configuration
	cfgFile, err := os.Open(*cfgFilePath)
	if err != nil {
		log.Println("Check DB configuration - stopping server")
	}
	defer cfgFile.Close()
	cfgBytes, _ := ioutil.ReadAll(cfgFile)
	err = json.Unmarshal(cfgBytes, &cfg.AppCfg)
	if err != nil {
		log.Println("Check application configuration - stopping server")
	}
	log.Println("App config:\n", string(cfgBytes))

	// Create DB connections
	connected := dbApi.CreateCouchDBConnections(cfg.AppCfg.Db)
	if connected != cfg.AppCfg.Db.NumOfConn {
		log.Println("Check DB connections - stopping server")
		return
	}

	// Start server
	go func() {
		connPort := ":" + strconv.Itoa(cfg.GetAppPort())
		log.Println("Running server..., port=", connPort)
		router := appApi.NewRouter()
		h2s := &http2.Server{}
		server := &http.Server{
			Addr:    connPort,
			Handler: h2c.NewHandler(router, h2s),
		}
		errs <- server.ListenAndServe()
	}()

	// For profiling
	profilePort := strconv.Itoa(cfg.GetPorfilePort())
	log.Println("Running profile server..., port=", profilePort)
	go StartProfileServer("localhost", profilePort)

	//select {}
	err = <-errs
	log.Println("Exiting server, error=", err.Error())

	//Close DB connections gracefully
	dbApi.CloseDbConnections()
}

// For profiling
func StartProfileServer(host, port string) {
	lpprofHostPort := host + ":" + port
	lErr := http.ListenAndServe(lpprofHostPort, nil)
	if lErr != nil {
		log.Print("StartProfileServer", "pprof Handler failed with lErr:", lErr)
	} else {
		log.Print("StartProfileServer", "Started pprof Handlerat Port", port)
	}
}

// Generates compiled regular expression
func GenerateRegExpMap(regExTypeMap map[string]string) {
	for patternType, pattern := range regExTypeMap {
		reg, err := regexp.Compile(pattern)
		if err != nil {
			log.Print("GenerateRegExpMap", "error : ", err.Error())
			continue
		}
		cc.CompiledRegExMap[patternType] = reg
	}
}
