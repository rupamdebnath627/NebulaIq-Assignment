package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
)

type Record struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Global variables to hold our processed data and ensure thread safety
var (
	processedRecords []Record
	mu               sync.Mutex
)

func main() {
	// Read data from the JSON file
	fileData, err := os.ReadFile("./../data.json")
	if err != nil {
		log.Fatalf("Error reading file: %v\n", err)
	}

	// Unmarshal (parse) the JSON into a slice of structs
	var records []Record
	if err := json.Unmarshal(fileData, &records); err != nil {
		log.Fatalf("Error parsing JSON: %v\n", err)
	}

	// Process data using multiple goroutines
	var wg sync.WaitGroup

	log.Println("Processing records...")
	for _, r := range records {
		wg.Add(1)

		// Launch a goroutine for each record
		go func(rec Record) {
			defer wg.Done()

			// Simulate some data processing
			rec.Name = fmt.Sprintf("Processed User: %s", rec.Name)

			// Safely append to the global slice using a Mutex to prevent race conditions
			mu.Lock()
			processedRecords = append(processedRecords, rec)
			mu.Unlock()
		}(r)
	}

	// Wait for all goroutines to finish processing before starting the server
	wg.Wait()
	log.Println("All records processed successfully.")

	// Expose REST endpoint
	http.HandleFunc("/records", handleRecords)

	log.Println("Server starting on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// handleRecords handles the HTTP requests to the /records endpoint
func handleRecords(w http.ResponseWriter, r *http.Request) {
	// Graceful Error Handling: Ensure it's a GET request
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed. Use GET.", http.StatusMethodNotAllowed)
		return
	}

	// Set the response header to indicate we are returning JSON
	w.Header().Set("Content-Type", "application/json")

	// Lock the mutex while reading the shared slice to be completely thread-safe
	mu.Lock()
	defer mu.Unlock()

	// Encode our processed records back into JSON and send them to the client
	if err := json.NewEncoder(w).Encode(processedRecords); err != nil {
		// Graceful Error Handling: If encoding fails, send a 500 error to the client
		http.Error(w, "Failed to encode response data", http.StatusInternalServerError)
		log.Printf("JSON encoding error: %v", err)
	}
}