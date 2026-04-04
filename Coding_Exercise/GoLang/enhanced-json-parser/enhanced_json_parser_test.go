package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleRecords(t *testing.T) {
	// Setup: Pre-populate our global state so the handler has something to return
	mu.Lock()
	processedRecords = []Record{
		{ID: 1, Name: "Processed: Alice"},
		{ID: 2, Name: "Processed: Bob"},
	}
	mu.Unlock()

	// --- TABLE-DRIVEN TEST SETUP ---
	tests := []struct {
		name           string       
		method         string       
		expectedStatus int         
		validateBody   bool         
	}{
		{
			name:           "Valid GET Request",
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			validateBody:   true,
		},
		{
			name:           "Invalid POST Request",
			method:         http.MethodPost,
			expectedStatus: http.StatusMethodNotAllowed,
			validateBody:   false,
		},
		{
			name:           "Invalid PUT Request",
			method:         http.MethodPut,
			expectedStatus: http.StatusMethodNotAllowed,
			validateBody:   false,
		},
	}

	// Loop through the table and run each test
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a simulated HTTP request
			req, err := http.NewRequest(tc.method, "/records", nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			// httptest.ResponseRecorder acts like our ResponseWriter
			rr := httptest.NewRecorder()

			// Call the handler directly
			handleRecords(rr, req)

			// Assert Status Code
			if rr.Code != tc.expectedStatus {
				t.Errorf("Expected status %d, got %d", tc.expectedStatus, rr.Code)
			}

			// Assert Body (if applicable for the test case)
			if tc.validateBody {
				var responseRecords []Record
				if err := json.NewDecoder(rr.Body).Decode(&responseRecords); err != nil {
					t.Fatalf("Failed to decode response body: %v", err)
				}

				if len(responseRecords) != 2 {
					t.Errorf("Expected 2 records, got %d", len(responseRecords))
				}

				if responseRecords[0].Name != "Processed: Alice" {
					t.Errorf("Unexpected record name: %s", responseRecords[0].Name)
				}
			}
		})
	}
}