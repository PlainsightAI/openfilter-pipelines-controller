/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-runner/api/v1alpha1"
)

// Server handles HTTP requests for pipeline runs
type Server struct {
	client client.Client
	port   int
}

// NewServer creates a new HTTP server instance
func NewServer(client client.Client, port int) *Server {
	return &Server{
		client: client,
		port:   port,
	}
}

// PipelineRunRequest represents the request body for creating a pipeline run
type PipelineRunRequest struct {
	// Additional parameters can be added here as needed
}

// PipelineRunResponse represents the response body for a pipeline run
type PipelineRunResponse struct {
	Namespace    string   `json:"namespace"`
	PipelineName string   `json:"pipelineName"`
	Files        []string `json:"files"`
	Count        int      `json:"count"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// Start starts the HTTP server and implements manager.Runnable interface
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	// Use separate path parameters for namespace and name
	mux.HandleFunc("POST /pipelines/{namespace}/{name}/runs", s.handlePipelineRun)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	logger := log.FromContext(ctx)
	logger.Info("Starting HTTP server", "port", s.port)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		logger.Info("Shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error(err, "Error shutting down HTTP server")
			return err
		}
		return nil
	case err := <-errChan:
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}
}

// handlePipelineRun handles POST /pipelines/{namespace}/{name}/runs
func (s *Server) handlePipelineRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := log.FromContext(ctx)

	// Extract namespace and name from path parameters
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	// Validate that both namespace and name are non-empty
	if namespace == "" || name == "" {
		writeError(w, http.StatusBadRequest, "both namespace and name must be non-empty")
		return
	}

	logger.Info("Handling pipeline run request", "namespace", namespace, "pipeline", name)

	// Load the pipeline resource
	pipeline := &pipelinesv1alpha1.Pipeline{}
	if err := s.client.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, pipeline); err != nil {
		logger.Error(err, "Failed to get pipeline", "namespace", namespace, "pipeline", name)
		writeError(w, http.StatusNotFound, fmt.Sprintf("pipeline %s/%s not found: %v", namespace, name, err))
		return
	}

	// Get credentials from secret if specified
	var accessKey, secretKey string
	if pipeline.Spec.Input.CredentialsSecret != nil {
		var err error
		accessKey, secretKey, err = s.getCredentials(ctx, pipeline)
		if err != nil {
			logger.Error(err, "Failed to get credentials")
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get credentials: %v", err))
			return
		}
	}

	// List files in the bucket
	files, err := s.listBucketFiles(ctx, pipeline, accessKey, secretKey)
	if err != nil {
		logger.Error(err, "Failed to list bucket files")
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list bucket files: %v", err))
		return
	}

	// Return response
	response := PipelineRunResponse{
		Namespace:    namespace,
		PipelineName: name,
		Files:        files,
		Count:        len(files),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error(err, "Failed to encode response")
	}
}

// getCredentials retrieves credentials from the specified secret
func (s *Server) getCredentials(ctx context.Context, pipeline *pipelinesv1alpha1.Pipeline) (string, string, error) {
	secretRef := pipeline.Spec.Input.CredentialsSecret
	namespace := secretRef.Namespace
	if namespace == "" {
		namespace = pipeline.Namespace
	}

	secret := &corev1.Secret{}
	if err := s.client.Get(ctx, types.NamespacedName{
		Name:      secretRef.Name,
		Namespace: namespace,
	}, secret); err != nil {
		return "", "", fmt.Errorf("failed to get secret %s/%s: %w", namespace, secretRef.Name, err)
	}

	accessKey := string(secret.Data["accessKeyId"])
	secretKey := string(secret.Data["secretAccessKey"])

	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("secret %s/%s missing required keys 'accessKeyId' or 'secretAccessKey'", namespace, secretRef.Name)
	}

	return accessKey, secretKey, nil
}

// listBucketFiles lists all files in the bucket specified in the pipeline
func (s *Server) listBucketFiles(ctx context.Context, pipeline *pipelinesv1alpha1.Pipeline, accessKey, secretKey string) ([]string, error) {
	input := pipeline.Spec.Input

	// Determine endpoint - use HTTPS by default unless explicitly http://
	endpoint := input.Endpoint
	useSSL := true
	if endpoint != "" {
		// Check if endpoint starts with http://
		if len(endpoint) > 7 && endpoint[:7] == "http://" {
			useSSL = false
			endpoint = endpoint[7:] // Remove http:// prefix
		} else if len(endpoint) > 8 && endpoint[:8] == "https://" {
			endpoint = endpoint[8:] // Remove https:// prefix
		}
	}

	// Create credentials
	var creds *credentials.Credentials
	if accessKey != "" && secretKey != "" {
		creds = credentials.NewStaticV4(accessKey, secretKey, "")
	} else {
		// Use anonymous credentials if none provided
		creds = credentials.NewStaticV4("", "", "")
	}

	// Configure custom transport if TLS verification should be skipped
	var customTransport http.RoundTripper
	if input.InsecureSkipTLSVerify {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		customTransport = transport
	}

	// Initialize MinIO client
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    useSSL,
		Region:    input.Region,
		Transport: customTransport,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	// List objects in the bucket
	var files []string
	objectCh := minioClient.ListObjects(ctx, input.Bucket, minio.ListObjectsOptions{
		Prefix:    input.Prefix,
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %w", object.Err)
		}
		files = append(files, object.Key)
	}

	return files, nil
}

// writeError writes an error response
func writeError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}
