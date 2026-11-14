package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHealthHandlerReportsDraining(t *testing.T) {
	var draining atomic.Bool
	sockets := int64(0)
	h := healthHandler(&draining, func() int64 { return sockets })

	// Serving normally.
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("healthy status = %d, want 200", rec.Code)
	}

	// Draining with work still in flight. A deploy script polls this to decide
	// whether the instance can be removed; 200 here would let it kill live
	// conversations, which is the whole failure this exists to prevent.
	draining.Store(true)
	sockets = 3
	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("draining status = %d, want 503", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "3 websockets open") {
		t.Errorf("body = %q, want the count of what is still open", body)
	}
}
