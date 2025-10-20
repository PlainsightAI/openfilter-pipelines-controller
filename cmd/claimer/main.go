package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/valkey-io/valkey-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// Environment variables
	EnvValkeyURL    = "VALKEY_URL"
	EnvStream       = "STREAM"
	EnvGroup        = "GROUP"
	EnvConsumerName = "CONSUMER_NAME"
	EnvPodName      = "POD_NAME"
	EnvPodNamespace = "POD_NAMESPACE"
	EnvS3Bucket     = "S3_BUCKET"
	EnvS3Endpoint   = "S3_ENDPOINT"
	EnvS3Region     = "S3_REGION"
	EnvS3AccessKey  = "S3_ACCESS_KEY_ID"
	EnvS3SecretKey  = "S3_SECRET_ACCESS_KEY"
	EnvS3PathStyle  = "S3_USE_PATH_STYLE"
	EnvS3SkipTLS    = "S3_INSECURE_SKIP_TLS_VERIFY"
	EnvVideoInput   = "VIDEO_INPUT_PATH"

	// Volume mount paths
	defaultInputPath = "/ws/input.mp4"

	// Pod annotations
	AnnotationMessageID = "queue.valkey.mid"
	AnnotationFile      = "queue.file"
	AnnotationAttempts  = "queue.attempts"
)

// Message represents a work item from Valkey Stream
type Message struct {
	Run      string
	File     string
	Attempts int
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Claimer starting...")

	if err := run(); err != nil {
		log.Fatalf("Claimer failed: %v", err)
	}

	log.Println("Claimer completed successfully")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load configuration from environment
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create Valkey client (with retry for outages during startup)
	var valkeyClient valkey.Client
	clientBackoff := newExponentialBackoff(1*time.Second, 30*time.Second)
	for {
		valkeyClient, err = createValkeyClient(cfg)
		if err == nil {
			clientBackoff.Reset()
			break
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isRetryableValkeyError(err) {
			return fmt.Errorf("failed to create Valkey client: %w", err)
		}

		delay := clientBackoff.Next()
		log.Printf("Valkey client creation failed: %v; retrying in %s", err, delay)
		if err := sleepWithContext(ctx, delay); err != nil {
			return err
		}
	}
	defer valkeyClient.Close()

	// Wait until Valkey is reachable; transient outages should keep the init container alive.
	if err := waitForValkey(ctx, valkeyClient); err != nil {
		return fmt.Errorf("failed to connect to Valkey: %w", err)
	}
	log.Println("Connected to Valkey")

	// Claim a message from the stream
	msg, messageID, err := claimMessage(ctx, valkeyClient, cfg)
	if err != nil {
		return fmt.Errorf("failed to claim message: %w", err)
	}
	log.Printf("Claimed message ID=%s, file=%s, attempts=%d", messageID, msg.File, msg.Attempts)

	// Create MinIO client
	minioClient, err := createMinIOClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	// Download file from S3
	if err := downloadFile(ctx, minioClient, cfg.S3Bucket, msg.File, cfg.VideoInputPath); err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	log.Printf("Downloaded file %s to %s", msg.File, cfg.VideoInputPath)

	// Patch pod annotations
	if err := patchPodAnnotations(ctx, cfg, messageID, msg); err != nil {
		return fmt.Errorf("failed to patch pod annotations: %w", err)
	}
	log.Println("Patched pod annotations")

	return nil
}

type Config struct {
	ValkeyURL      string
	Stream         string
	Group          string
	ConsumerName   string
	PodName        string
	PodNamespace   string
	S3Bucket       string
	S3Endpoint     string
	S3Region       string
	S3AccessKey    string
	S3SecretKey    string
	S3UsePathStyle bool
	S3SkipTLS      bool
	VideoInputPath string
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		ValkeyURL:    getEnvOrDefault(EnvValkeyURL, "localhost:6379"),
		Stream:       os.Getenv(EnvStream),
		Group:        os.Getenv(EnvGroup),
		ConsumerName: getEnvOrDefault(EnvConsumerName, "claimer"),
		PodName:      os.Getenv(EnvPodName),
		PodNamespace: os.Getenv(EnvPodNamespace),
		S3Bucket:     os.Getenv(EnvS3Bucket),
		S3Endpoint:   os.Getenv(EnvS3Endpoint),
		S3Region:     getEnvOrDefault(EnvS3Region, "us-east-1"),
		S3AccessKey:  os.Getenv(EnvS3AccessKey),
		S3SecretKey:  os.Getenv(EnvS3SecretKey),
		VideoInputPath: func() string {
			if value := os.Getenv(EnvVideoInput); value != "" {
				return value
			}
			return defaultInputPath
		}(),
	}

	// Parse boolean flags
	cfg.S3UsePathStyle = getEnvOrDefault(EnvS3PathStyle, "false") == "true"
	cfg.S3SkipTLS = getEnvOrDefault(EnvS3SkipTLS, "false") == "true"

	// Validate required fields
	if cfg.Stream == "" {
		return nil, fmt.Errorf("STREAM is required")
	}
	if cfg.Group == "" {
		return nil, fmt.Errorf("GROUP is required")
	}
	if cfg.PodName == "" {
		return nil, fmt.Errorf("POD_NAME is required")
	}
	if cfg.PodNamespace == "" {
		return nil, fmt.Errorf("POD_NAMESPACE is required")
	}
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET is required")
	}
	if cfg.VideoInputPath == "" {
		return nil, fmt.Errorf("VIDEO_INPUT_PATH is required")
	}

	return cfg, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func createValkeyClient(cfg *Config) (valkey.Client, error) {
	clientOpts := valkey.ClientOption{
		InitAddress: []string{cfg.ValkeyURL},
	}

	client, err := valkey.NewClient(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create Valkey client: %w", err)
	}

	return client, nil
}

func pingValkey(ctx context.Context, client valkey.Client) error {
	cmd := client.B().Ping().Build()
	return client.Do(ctx, cmd).Error()
}

func claimMessage(ctx context.Context, client valkey.Client, cfg *Config) (*Message, string, error) {
	backoff := newExponentialBackoff(1*time.Second, 30*time.Second)

	for {
		msg, messageID, err := claimMessageOnce(ctx, client, cfg)
		if err == nil {
			backoff.Reset()
			return msg, messageID, nil
		}

		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}

		if !isRetryableValkeyError(err) {
			return nil, "", err
		}

		delay := backoff.Next()
		log.Printf("Waiting for Valkey stream after error: %v; retrying in %s", err, delay)
		if err := sleepWithContext(ctx, delay); err != nil {
			return nil, "", err
		}
	}
}

func claimMessageOnce(ctx context.Context, client valkey.Client, cfg *Config) (*Message, string, error) {
	cmd := client.B().Xreadgroup().
		Group(cfg.Group, cfg.ConsumerName).
		Count(1).
		Block(0).
		Streams().
		Key(cfg.Stream).
		Id(">").
		Build()

	result := client.Do(ctx, cmd)
	if result.Error() != nil {
		return nil, "", fmt.Errorf("XREADGROUP failed: %w", result.Error())
	}

	streams, err := result.AsXRead()
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse XREADGROUP response: %w", err)
	}

	entries, ok := streams[cfg.Stream]
	if !ok || len(entries) == 0 {
		return nil, "", fmt.Errorf("no messages available in stream")
	}

	entry := entries[0]
	messageID := entry.ID
	values := entry.FieldValues

	msg := &Message{
		Run:  values["run"],
		File: values["file"],
	}

	if attemptsStr, ok := values["attempts"]; ok {
		attempts, err := strconv.Atoi(attemptsStr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid attempts value: %w", err)
		}
		msg.Attempts = attempts
	}

	if msg.File == "" {
		return nil, "", fmt.Errorf("missing 'file' field in message")
	}

	return msg, messageID, nil
}

func createMinIOClient(cfg *Config) (*minio.Client, error) {
	// Determine endpoint and SSL settings
	endpoint := cfg.S3Endpoint
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
	if cfg.S3AccessKey != "" && cfg.S3SecretKey != "" {
		creds = credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, "")
	} else {
		// Use anonymous credentials if none provided
		creds = credentials.NewStaticV4("", "", "")
	}

	// Configure custom transport if TLS verification should be skipped
	var customTransport http.RoundTripper
	if cfg.S3SkipTLS {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
		}
		customTransport = transport
	}

	// Initialize MinIO client
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    useSSL,
		Region:    cfg.S3Region,
		Transport: customTransport,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	return minioClient, nil
}

func downloadFile(ctx context.Context, client *minio.Client, bucket, key, destPath string) error {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Get object from S3
	object, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer object.Close()

	// Create destination file
	file, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Copy content
	if _, err := io.Copy(file, object); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func patchPodAnnotations(ctx context.Context, cfg *Config, messageID string, msg *Message) error {
	// Create in-cluster Kubernetes client
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to create in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Build patch for annotations
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				AnnotationMessageID: messageID,
				AnnotationFile:      msg.File,
				AnnotationAttempts:  strconv.Itoa(msg.Attempts),
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	// Apply patch to pod
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err = clientset.CoreV1().Pods(cfg.PodNamespace).Patch(
		ctx,
		cfg.PodName,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to patch pod: %w", err)
	}

	return nil
}

func waitForValkey(ctx context.Context, client valkey.Client) error {
	backoff := newExponentialBackoff(1*time.Second, 30*time.Second)

	for {
		if err := pingValkey(ctx, client); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !isRetryableValkeyError(err) {
				return err
			}

			delay := backoff.Next()
			log.Printf("Valkey unavailable: %v; retrying in %s", err, delay)
			if err := sleepWithContext(ctx, delay); err != nil {
				return err
			}
			continue
		}

		backoff.Reset()
		return nil
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isRetryableValkeyError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	if errors.Is(err, io.EOF) {
		return true
	}

	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "connection refused"),
		strings.Contains(errMsg, "connection reset"),
		strings.Contains(errMsg, "broken pipe"),
		strings.Contains(errMsg, "network is unreachable"),
		strings.Contains(errMsg, "no route to host"),
		strings.Contains(errMsg, "host is down"),
		strings.Contains(errMsg, "i/o timeout"),
		strings.Contains(errMsg, "temporary failure in name resolution"):
		return true
	case strings.Contains(errMsg, "NOAUTH"),
		strings.Contains(errMsg, "WRONGPASS"),
		strings.Contains(errMsg, "NOGROUP"):
		return false
	}

	return false
}

type exponentialBackoff struct {
	min     time.Duration
	max     time.Duration
	current time.Duration
}

func newExponentialBackoff(min, max time.Duration) *exponentialBackoff {
	if min <= 0 {
		min = time.Second
	}
	if max < min {
		max = min
	}

	return &exponentialBackoff{
		min:     min,
		max:     max,
		current: min,
	}
}

func (b *exponentialBackoff) Next() time.Duration {
	delay := b.current
	if b.current < b.max {
		b.current *= 2
		if b.current > b.max {
			b.current = b.max
		}
	}
	return delay
}

func (b *exponentialBackoff) Reset() {
	b.current = b.min
}
