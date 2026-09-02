package main

import (
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

	jobsHandler(recorder, req)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestJobsHandlerRejectsInvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":`))
	recorder := httptest.NewRecorder()

	jobsHandler(recorder, req)

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

	jobsHandler(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusOK,
			recorder.Code,
		)
	}

	if recorder.Body.String() != "OK" {
		t.Errorf(
			"expected body %q, got %q",
			"OK",
			recorder.Body.String(),
		)
	}
}
