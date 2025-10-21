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

package controller

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/queue"
)

const (
	// Annotation keys for pod queue metadata
	AnnotationMessageID = "queue.valkey.mid"
	AnnotationFile      = "queue.file"
	AnnotationAttempts  = "queue.attempts"

	// Annotation values
	AnnotationValueTrue = "true"

	// Condition types
	ConditionTypeProgressing = "Progressing"
	ConditionTypeSucceeded   = "Succeeded"
	ConditionTypeDegraded    = "Degraded"

	// Reconciliation intervals
	StatusUpdateInterval = 30

	// DefaultVideoInputPath is where the claimer stores downloaded artifacts when not overridden.
	DefaultVideoInputPath = "/ws/input.mp4"
)

// ValkeyClientInterface defines the interface for Valkey operations
type ValkeyClientInterface interface {
	CreateStreamAndGroup(ctx context.Context, streamKey, groupName string) error
	GetStreamLength(ctx context.Context, streamKey string) (int64, error)
	GetConsumerGroupLag(ctx context.Context, streamKey, groupName string) (int64, error)
	GetPendingCount(ctx context.Context, streamKey, groupName string) (int64, error)
	AckMessage(ctx context.Context, streamKey, groupName, messageID string) error
	EnqueueFileWithAttempts(ctx context.Context, streamKey, runID, filepath string, attempts int) (string, error)
	AddToDLQ(ctx context.Context, dlqKey, runID, filepath string, attempts int, reason string) error
	AutoClaim(ctx context.Context, streamKey, groupName, consumerName string, minIdleTime int64, count int64) ([]queue.XMessage, error)
	ReadRange(ctx context.Context, streamKey, start, end string, count int64) ([]queue.XMessage, error)
	DeleteMessages(ctx context.Context, streamKey string, messageIDs ...string) error
}

// PipelineRunReconciler reconciles a PipelineRun object
type PipelineRunReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	ValkeyClient   ValkeyClientInterface
	ValkeyAddr     string
	ValkeyPassword string
	ClaimerImage   string // Image for the claimer init container
}

// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=replicasets/status,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// The reconciler branches on Pipeline mode (Batch or Stream) and delegates to
// mode-specific reconciliation functions defined in:
// - pipelinerun_controller_batch.go: Batch mode reconciliation
// - pipelinerun_controller_streaming.go: Streaming mode reconciliation
func (r *PipelineRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the PipelineRun
	pipelineRun := &pipelinesv1alpha1.PipelineRun{}
	if err := r.Get(ctx, req.NamespacedName, pipelineRun); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("PipelineRun resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get PipelineRun")
		return ctrl.Result{}, err
	}

	// Get the Pipeline resource
	pipeline, err := r.getPipeline(ctx, pipelineRun)
	if err != nil {
		log.Error(err, "Failed to get Pipeline")
		r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "PipelineNotFound", err.Error())
		if err := r.Status().Update(ctx, pipelineRun); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Branch by pipeline mode
	mode := pipeline.Spec.Mode
	if mode == "" {
		mode = pipelinesv1alpha1.PipelineModeBatch // default
	}

	if mode == pipelinesv1alpha1.PipelineModeStream {
		return r.reconcileStreaming(ctx, pipelineRun, pipeline)
	}

	// Default: Batch mode
	return r.reconcileBatch(ctx, pipelineRun, pipeline)
}

// Helper functions shared between batch and streaming modes are defined below.
// Mode-specific reconciliation logic is in:
// - pipelinerun_controller_batch.go
// - pipelinerun_controller_streaming.go

// getCredentials retrieves S3 credentials for the pipeline source secret, if configured.
func (r *PipelineRunReconciler) getCredentials(ctx context.Context, pipeline *pipelinesv1alpha1.Pipeline) (string, string, error) {
	if pipeline.Spec.Source.Bucket == nil {
		return "", "", nil
	}

	secretRef := pipeline.Spec.Source.Bucket.CredentialsSecret
	if secretRef == nil {
		return "", "", nil
	}

	namespace := secretRef.Namespace
	if namespace == "" {
		namespace = pipeline.Namespace
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretRef.Name, Namespace: namespace}, secret); err != nil {
		return "", "", fmt.Errorf("failed to get secret %s/%s: %w", namespace, secretRef.Name, err)
	}

	accessKey := string(secret.Data["accessKeyId"])
	secretKey := string(secret.Data["secretAccessKey"])

	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("secret %s/%s missing required keys 'accessKeyId' or 'secretAccessKey'", namespace, secretRef.Name)
	}

	return accessKey, secretKey, nil
}

// listBucketFiles lists objects available to process for the pipeline source configuration.
func (r *PipelineRunReconciler) listBucketFiles(ctx context.Context, pipeline *pipelinesv1alpha1.Pipeline, accessKey, secretKey string) ([]string, error) {
	if pipeline.Spec.Source.Bucket == nil {
		return nil, fmt.Errorf("pipeline has no bucket source configured")
	}

	bucket := pipeline.Spec.Source.Bucket

	endpoint := bucket.Endpoint
	useSSL := true
	if endpoint != "" {
		if len(endpoint) > 7 && endpoint[:7] == "http://" {
			useSSL = false
			endpoint = endpoint[7:]
		} else if len(endpoint) > 8 && endpoint[:8] == "https://" {
			endpoint = endpoint[8:]
		}
	}

	var creds *credentials.Credentials
	if accessKey != "" && secretKey != "" {
		creds = credentials.NewStaticV4(accessKey, secretKey, "")
	} else {
		creds = credentials.NewStaticV4("", "", "")
	}

	var customTransport http.RoundTripper
	if bucket.InsecureSkipTLSVerify {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		customTransport = transport
	}

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    useSSL,
		Region:    bucket.Region,
		Transport: customTransport,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	files := make([]string, 0, 100) // Pre-allocate with reasonable initial capacity
	objectCh := minioClient.ListObjects(ctx, bucket.Name, minio.ListObjectsOptions{
		Prefix:    bucket.Prefix,
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

// getPipeline retrieves the Pipeline resource referenced by the PipelineRun
func (r *PipelineRunReconciler) getPipeline(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) (*pipelinesv1alpha1.Pipeline, error) {
	namespace := pipelineRun.Namespace
	if pipelineRun.Spec.PipelineRef.Namespace != nil {
		namespace = *pipelineRun.Spec.PipelineRef.Namespace
	}

	pipeline := &pipelinesv1alpha1.Pipeline{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      pipelineRun.Spec.PipelineRef.Name,
		Namespace: namespace,
	}, pipeline); err != nil {
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}

	return pipeline, nil
}

// setCondition sets or updates a condition in the PipelineRun status
func (r *PipelineRunReconciler) setCondition(pipelineRun *pipelinesv1alpha1.PipelineRun, conditionType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: pipelineRun.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}

	meta.SetStatusCondition(&pipelineRun.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pipelinesv1alpha1.PipelineRun{}).
		Owns(&batchv1.Job{}).
		Owns(&appsv1.Deployment{}).
		Named("pipelinerun").
		Complete(r)
}
