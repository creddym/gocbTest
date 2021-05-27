package cfg

type AppCfg_t struct {
	App App `json:"app"`
	Db  Db  `json:"db"`
}

type App struct {
	Port        int    `json:"port"`
	ProfilePort int    `json:"profilePort"`
	LogLevel    string `json:"logLevel"`
}

type Db struct {
	URL               string `json:"url"`
	Bucket            string `json:"bucket"`
	User              string `json:"user"`
	Passwd            string `json:"passwd"`
	NumOfConn         int    `json:"numOfConn"`
	TolelateLatencyMs int    `json:"tolelateLatencyMs"`
	EnableLog         bool   `json:"enableLog"`
	GocbSdkVersion    string `json:"gocbSdkVersion"`
}

var AppCfg AppCfg_t

func GetAppCfg() *App {
	return &AppCfg.App
}

func GetDbCfg() *Db {
	return &AppCfg.Db
}

func GetNumOfConn() int {
	return AppCfg.Db.NumOfConn
}

func GetTolelateLatencyMs() int {
	return AppCfg.Db.TolelateLatencyMs
}

func GetEnableLog() bool {
	return AppCfg.Db.EnableLog
}

func GetGocbSdkVersion() string {
	return AppCfg.Db.GocbSdkVersion
}

func GetGocbSdkVersionFromDbCfg(db Db) string {
	return db.GocbSdkVersion
}

func GetAppPort() int {
	return AppCfg.App.Port
}

func GetPorfilePort() int {
	return AppCfg.App.ProfilePort
}
