// Command api will accept and expose workflow jobs over HTTP.
package main

import (
	"crypto/rand"
	"distributed-workflow-engine/internal/jobs"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Write([]byte("OK"))
}

func (a *api) jobsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var input createJobRequest

	decoder := json.NewDecoder(r.Body)

	err := decoder.Decode(&input)

	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(input.Type) == "" {
		http.Error(w, "Job type is required", http.StatusBadRequest)
		return
	}

	id, err := newJobID()
	if err != nil {
		http.Error(w, "Could not generate job ID", http.StatusInternalServerError)
		return
	}

	job := jobs.Job{
		ID:        id,
		Type:      strings.TrimSpace(input.Type), // The client will only be allowed to write the data for the TYPE
		Payload:   input.Payload,                 // and the Payload
		Status:    jobs.StatusPending,
		CreatedAt: time.Now().UTC(),
	}

	a.store.Save(job) // Save the new job in this API instance's shared store.

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	err = json.NewEncoder(w).Encode(job)
	if err != nil {
		log.Printf("could not encode job response: %v", err)
	}
}

func newJobID() (string, error) {
	byteSlice := make([]byte, 16)
	_, err := rand.Read(byteSlice)

	if err != nil {
		return "", err
	}

	return hex.EncodeToString(byteSlice), nil
}

type createJobRequest struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

type api struct {
	store *jobs.Store
}

func main() {
	app := &api{
		store: jobs.NewStore(), // Give this API instance one shared job store
	}

	// Makes it so that healthHandler handles requests whose path is /health
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/jobs", app.jobsHandler)
	log.Println("Server starting on port :8080...")

	err := http.ListenAndServe(":8080", nil)

	if err != nil {
		log.Fatal(err)
	}

}
