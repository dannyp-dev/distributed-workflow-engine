// Command api will accept and expose workflow jobs over HTTP.
package main

import (
	"log"
	"net/http"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Write([]byte("OK"))
}

func jobsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Write([]byte("OK"))
}

func main() {
	// Makes it so that healthHandler handles requests whose path is /health
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/jobs", jobsHandler)
	log.Println("Server starting on port :8080...")

	err := http.ListenAndServe(":8080", nil)

	if err != nil {
		log.Fatal(err)
	}

}
