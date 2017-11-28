package conf

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

var Setting struct {
	Dsn      string
	FilePath string
}

func init() {
	data, err := ioutil.ReadFile("conf/setting.json")
	if err != nil {
		log.Fatal("%v", err)
	}

	err = json.Unmarshal(data, &Setting)
	if err != nil {
		log.Fatal("%v", err)
	}
}
