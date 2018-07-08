package gdsb

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

var heartbeatResp = heartbeatResponse{Status: "OK", Code: 200}

//HeartbeatHandler for handeling
func HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(heartbeatResp)
}

//HeartbeatResponse is the rest api HeartbeatResponse
type heartbeatResponse struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
}

//HeartbeatResponseService will start a app info service for
func HeartbeatResponseService(address string) error {
	r := mux.NewRouter()
	r.HandleFunc("/", HeartbeatHandler).Methods("GET")
	srv := &http.Server{
		Handler:      r,
		Addr:         address,
		WriteTimeout: 100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
	}
	err := srv.ListenAndServe()
	if err != nil {
		log.Printf("Error while starting App Info Service. Error: %v\n", err)
	}
	return err
}
