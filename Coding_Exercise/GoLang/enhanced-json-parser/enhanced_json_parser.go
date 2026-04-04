package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Record represents our data structure
type Record struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Global state (in a real app, this would likely be in a database or struct field)
var (
	processedRecords []Record
	mu               sync.Mutex
)

// --- CUSTOM ERROR TYPE ---
type AppError struct {
	StatusCode int
	Message    string
	Err        error
}

// Implement the standard error interface
func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func main() {
	// --- STRUCTURED LOGGING ---
	// Set up slog to output JSON format to standard out
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger) // Make it the default logger

	slog.Info("Initializing application...")

	// 1. Load Data
	if err := loadAndProcessData(); err != nil {
		slog.Error("Failed to initialize data", "error", err)
		os.Exit(1)
	}

	// 2. Setup Server Route
	mux := http.NewServeMux()
	mux.HandleFunc("/records", handleRecords)

	// Create a custom server struct to configure it explicitly
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// --- GRACEFUL SHUTDOWN ---
	// Create a channel to listen for OS signals (like Ctrl+C or Docker stop)
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// Start the server in a separate goroutine so it doesn't block
	go func() {
		slog.Info("Server started", "port", 8080)
		// ErrServerClosed is expected when we call Shutdown()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Server forced to shutdown", "error", err)
		}
	}()

	// Block main thread until we receive a signal
	<-stopChan
	slog.Info("Shutting down server gracefully...")

	// Create a context with a 5-second timeout to give active connections time to finish
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("Server shutdown failed", "error", err)
	} else {
		slog.Info("Server stopped elegantly.")
	}
}

// loadAndProcessData reads the JSON file and processes it concurrently
func loadAndProcessData() error {
	fileData, err := os.ReadFile("./../data.json")
	if err != nil {
		return &AppError{StatusCode: 500, Message: "Failed to read file", Err: err}
	}

	var records []Record
	if err := json.Unmarshal(fileData, &records); err != nil {
		return &AppError{StatusCode: 500, Message: "Failed to parse JSON", Err: err}
	}

	var wg sync.WaitGroup
	for _, r := range records {
		wg.Add(1)
		go func(rec Record) {
			defer wg.Done()
			rec.Name = "Processed: " + rec.Name
			
			mu.Lock()
			processedRecords = append(processedRecords, rec)
			mu.Unlock()
		}(r)
	}
	wg.Wait()
	
	slog.Info("Data processing complete", "record_count", len(processedRecords))
	return nil
}

// handleRecords handles the REST API request
func handleRecords(w http.ResponseWriter, r *http.Request) {
	// --- CONTEXT TIMEOUT ---
	// Create a context that will automatically cancel after 2 seconds
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if r.Method != http.MethodGet {
		appErr := &AppError{StatusCode: http.StatusMethodNotAllowed, Message: "Method not allowed"}
		slog.Warn("Invalid HTTP method", "method", r.Method)
		http.Error(w, appErr.Message, appErr.StatusCode)
		return
	}

	// We use a select statement to simulate checking if the context timed out
	// before or during our work
	select {
	case <-ctx.Done():
		slog.Error("Request timed out", "error", ctx.Err())
		http.Error(w, "Request timed out", http.StatusGatewayTimeout)
		return
	default:
		// Normal execution continues here if no timeout occurred
	}

	w.Header().Set("Content-Type", "application/json")
	
	mu.Lock()
	defer mu.Unlock()

	if err := json.NewEncoder(w).Encode(processedRecords); err != nil {
		slog.Error("Failed to encode response", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	} else {
		slog.Info("Successfully served records", "ip", r.RemoteAddr)
	}
}