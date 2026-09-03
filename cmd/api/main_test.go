package main

import (
	"distributed-workflow-engine/internal/jobs"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthHandlerGET(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder := httptest.NewRecorder()

	healthHandler(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	if recorder.Body.String() != "OK" {
		t.Errorf("expected body %q, got %q", "OK", recorder.Body.String())
	}
}

func TestHealthHandlerPOST(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	recorder := httptest.NewRecorder()

	healthHandler(recorder, req)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestJobsHandlerRejectsGET(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	recorder := httptest.NewRecorder()

	app := newTestAPI()
	app.jobsHandler(recorder, req)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestJobsHandlerRejectsInvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":`))
	recorder := httptest.NewRecorder()

	app := newTestAPI()
	app.jobsHandler(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			recorder.Code,
		)
	}
}

func TestJobsHandlerAcceptsValidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{
  "type": "resize_image",
  "payload": {
    "filename": "photo.jpg"
  }
}`))
	recorder := httptest.NewRecorder()

	app := newTestAPI()
	app.jobsHandler(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusCreated,
			recorder.Code,
		)
	}

	if recorder.Header().Get("Content-Type") != "application/json" {
		t.Errorf(
			"expected content type %q, got %q",
			"application/json",
			recorder.Header().Get("Content-Type"),
		)
	}

	var createdJob jobs.Job

	err := json.NewDecoder(recorder.Body).Decode(&createdJob)
	if err != nil {
		t.Fatalf("could not decode response body: %v", err)
	}

	storedJob, found := app.store.Get(createdJob.ID)
	if !found {
		t.Fatal("expected created job to be saved")
	}

	if storedJob.ID != createdJob.ID {
		t.Errorf(
			"expected stored job ID %q, got %q",
			createdJob.ID,
			storedJob.ID,
		)
	}

	if createdJob.ID == "" {
		t.Error("expected generated job ID")
	}

	if createdJob.Type != "resize_image" {
		t.Errorf(
			"expected job type %q, got %q",
			"resize_image",
			createdJob.Type,
		)
	}

	if createdJob.Status != jobs.StatusPending {
		t.Errorf(
			"expected job status %q, got %q",
			jobs.StatusPending,
			createdJob.Status,
		)
	}

	if createdJob.CreatedAt.IsZero() {
		t.Error("expected job creation time to be set")
	}
}

func TestJobsHandlerRejectsMissingType(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{"payload":{"filename":"photo.jpg"}}`),
	)
	recorder := httptest.NewRecorder()

	app := newTestAPI()
	app.jobsHandler(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			recorder.Code,
		)
	}
}

func newTestAPI() *api {
	return &api{
		store: jobs.NewStore(),
	}
}
