package gdsb

import (
	"encoding/json"
	"log"
	"sync"

	uuid "github.com/satori/go.uuid"
)

//Hold will hold execution
func Hold() {
	var wg = &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

//UUIDstring to generate UUID
func UUIDstring() string {
	u2, err := uuid.NewV4()
	if err != nil {
		log.Println("Error while generating UUID. Erro: ", err)
		return ""
	}
	return u2.String()
}

//ToJSON takes object o and returns string and error
func ToJSON(o interface{}) (string, error) {
	j, e := json.Marshal(o)
	if e != nil {
		return string(j), e
	}
	return "", e
}
