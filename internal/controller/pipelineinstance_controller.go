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
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"time"

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
	StatusUpdateInterval = 30 * time.Second

	// DefaultVideoInputPath is where the claimer stores downloaded artifacts when not overridden.
	DefaultVideoInputPath = "/ws/input.mp4"

	// DefaultValkeyOrgSecretName is the default name for per-org Valkey credentials secrets.
	DefaultValkeyOrgSecretName = "valkey-org-credentials"
)

// ValkeyClientInterface defines the interface for Valkey operations
type ValkeyClientInterface interface {
	CreateStreamAndGroup(ctx context.Context, streamKey, groupName string) error
	GetStreamLength(ctx context.Context, streamKey string) (int64, error)
	GetConsumerGroupLag(ctx context.Context, streamKey, groupName string) (int64, error)
	GetPendingCount(ctx context.Context, streamKey, groupName string) (int64, error)
	GetPendingForConsumer(ctx context.Context, streamKey, groupName, consumer string, count int64) ([]string, error)
	GetPendingEntryDetails(ctx context.Context, streamKey, groupName string, minIdleTime int64, count int64) ([]queue.PendingEntry, error)
	AckMessage(ctx context.Context, streamKey, groupName, messageID string) error
	EnqueueFileWithAttempts(ctx context.Context, streamKey, runID, filepath string, attempts int) (string, error)
	AddToDLQ(ctx context.Context, dlqKey, runID, filepath string, attempts int, reason string) error
	AutoClaim(ctx context.Context, streamKey, groupName, consumerName string, minIdleTime int64, count int64) ([]queue.XMessage, error)
	ClaimMessages(ctx context.Context, streamKey, groupName, consumerName string, minIdleTime int64, messageIDs ...string) ([]queue.XMessage, error)
	ReadRange(ctx context.Context, streamKey, start, end string, count int64) ([]queue.XMessage, error)
	DeleteMessages(ctx context.Context, streamKey string, messageIDs ...string) error
	EnsureACLUser(ctx context.Context, username, password, namespace string) error
	DeleteACLUser(ctx context.Context, username string) error
}

// PipelineInstanceReconciler reconciles a PipelineInstance object
type PipelineInstanceReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	ValkeyClient          ValkeyClientInterface
	ValkeyAddr            string
	ValkeyOrgSecretName   string            // Name of the per-org Valkey credentials secret (default: valkey-org-credentials)
	ClaimerImage          string            // Image for the claimer init container
	GPUNodeSelectorLabels map[string]string // Node selector labels applied to pods that request nvidia.com/gpu resources; nil disables the feature
}

// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelinesources,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=replicasets/status,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete

// ensureOrgValkeyCredentials ensures a per-org Valkey ACL user and corresponding
// secret exist in the target namespace. The ACL user is restricted to keys
// matching ns:<namespace>:* so it can only access its own org's data.
func (r *PipelineInstanceReconciler) ensureOrgValkeyCredentials(ctx context.Context, namespace string) error {
	log := logf.FromContext(ctx)

	secretName := r.ValkeyOrgSecretName
	if secretName == "" {
		secretName = DefaultValkeyOrgSecretName
	}

	username := queue.ValkeyUsernameForNamespace(namespace)

	// Check if the secret already exists
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, existing)
	if err == nil {
		// Secret exists — validate contents and ensure ACL user is up to date
		if existing.Data == nil {
			return fmt.Errorf("org Valkey secret %s/%s has no data", namespace, secretName)
		}
		storedUsername, ok := existing.Data["valkey-username"]
		if !ok {
			return fmt.Errorf("org Valkey secret %s/%s is missing key %q", namespace, secretName, "valkey-username")
		}
		if string(storedUsername) != username {
			return fmt.Errorf("org Valkey secret %s/%s has unexpected username %q, expected %q", namespace, secretName, string(storedUsername), username)
		}
		storedPassword, ok := existing.Data["valkey-password"]
		if !ok || len(storedPassword) == 0 {
			return fmt.Errorf("org Valkey secret %s/%s has missing or empty password", namespace, secretName)
		}
		if err := r.ValkeyClient.EnsureACLUser(ctx, username, string(storedPassword), namespace); err != nil {
			return fmt.Errorf("failed to ensure ACL user %s: %w", username, err)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check org Valkey secret in %s: %w", namespace, err)
	}

	// Generate a random password
	passwordBytes := make([]byte, 32)
	if _, err := rand.Read(passwordBytes); err != nil {
		return fmt.Errorf("failed to generate random password: %w", err)
	}
	password := base64.RawURLEncoding.EncodeToString(passwordBytes)

	// Create the secret first — the secret is the source of truth for the password.
	// If two reconciles race, the loser gets AlreadyExists and re-reads the winner's
	// password to ensure the ACL user matches.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "openfilter-pipelines-controller",
				"app.kubernetes.io/component":  "valkey-credentials",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"valkey-username": []byte(username),
			"valkey-password": []byte(password),
		},
	}
	if err := r.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create org Valkey secret in %s: %w", namespace, err)
		}
		// Race: another reconcile created it first — re-read to get the winning password
		if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
			return fmt.Errorf("failed to re-read org Valkey secret after race in %s: %w", namespace, err)
		}
		password = string(secret.Data["valkey-password"])
	}

	// Set the ACL user with the password from the secret (source of truth)
	if err := r.ValkeyClient.EnsureACLUser(ctx, username, password, namespace); err != nil {
		return fmt.Errorf("failed to create ACL user %s: %w", username, err)
	}

	log.Info("Created org Valkey credentials", "namespace", namespace, "username", username)
	return nil
}

// cleanupOrgValkeyCredentials removes the per-org Valkey ACL user and secret
// when the last PipelineInstance in a namespace is being deleted.
func (r *PipelineInstanceReconciler) cleanupOrgValkeyCredentials(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) {
	log := logf.FromContext(ctx)
	namespace := pipelineInstance.Namespace

	// List other PipelineInstances in the same namespace
	list := &pipelinesv1alpha1.PipelineInstanceList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		log.Error(err, "Failed to list PipelineInstances for ACL cleanup")
		return
	}

	// Count instances that are NOT being deleted (excluding the current one)
	active := 0
	for i := range list.Items {
		if list.Items[i].UID != pipelineInstance.UID && list.Items[i].DeletionTimestamp.IsZero() {
			active++
		}
	}
	if active > 0 {
		return // other active instances exist, keep the credentials
	}

	username := queue.ValkeyUsernameForNamespace(namespace)

	// Delete the ACL user from Valkey
	if err := r.ValkeyClient.DeleteACLUser(ctx, username); err != nil {
		log.Error(err, "Failed to delete Valkey ACL user", "username", username)
	} else {
		log.Info("Deleted Valkey ACL user", "username", username)
	}

	// Delete the org secret
	secretName := r.ValkeyOrgSecretName
	if secretName == "" {
		secretName = DefaultValkeyOrgSecretName
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err == nil {
		if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to delete org Valkey secret", "secret", secretName)
		} else {
			log.Info("Deleted org Valkey secret", "namespace", namespace)
		}
	}
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// The reconciler branches on Pipeline mode (Batch or Stream) and delegates to
// mode-specific reconciliation functions defined in:
// - pipelineinstance_controller_batch.go: Batch mode reconciliation
// - pipelineinstance_controller_streaming.go: Streaming mode reconciliation
func (r *PipelineInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the PipelineInstance
	pipelineInstance := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(ctx, req.NamespacedName, pipelineInstance); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("PipelineInstance resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get PipelineInstance")
		return ctrl.Result{}, err
	}

	// Clean up per-org Valkey credentials when the last PipelineInstance in a namespace is deleted
	if !pipelineInstance.DeletionTimestamp.IsZero() {
		r.cleanupOrgValkeyCredentials(ctx, pipelineInstance)
	}

	// Get the Pipeline resource
	pipeline, err := r.getPipeline(ctx, pipelineInstance)
	if err != nil {
		// If the PipelineInstance is being deleted, we need to proceed with cleanup even if Pipeline is missing
		if !pipelineInstance.DeletionTimestamp.IsZero() {
			log.Info("Pipeline not found but PipelineInstance is being deleted, proceeding with cleanup")
			// Attempt cleanup for both modes since we don't know which mode it was
			// Streaming mode uses finalizers, batch mode uses owner references
			// Try streaming cleanup first (it will no-op if no finalizer is present)
			return r.reconcileStreaming(ctx, pipelineInstance, nil, nil)
		}

		log.Error(err, "Failed to get Pipeline")
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "PipelineNotFound", err.Error())
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Get the PipelineSource resource
	pipelineSource, err := r.getPipelineSource(ctx, pipelineInstance)
	if err != nil {
		log.Error(err, "Failed to get PipelineSource")
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "PipelineSourceNotFound", err.Error())
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
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
		return r.reconcileStreaming(ctx, pipelineInstance, pipeline, pipelineSource)
	}

	// Default: Batch mode
	return r.reconcileBatch(ctx, pipelineInstance, pipeline, pipelineSource)
}

// Helper functions shared between batch and streaming modes are defined below.
// Mode-specific reconciliation logic is in:
// - pipelineinstance_controller_batch.go
// - pipelineinstance_controller_streaming.go

// getPipelineSource retrieves the PipelineSource resource referenced by the PipelineInstance
func (r *PipelineInstanceReconciler) getPipelineSource(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) (*pipelinesv1alpha1.PipelineSource, error) {
	namespace := pipelineInstance.Namespace
	if pipelineInstance.Spec.SourceRef.Namespace != nil {
		namespace = *pipelineInstance.Spec.SourceRef.Namespace
	}

	pipelineSource := &pipelinesv1alpha1.PipelineSource{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      pipelineInstance.Spec.SourceRef.Name,
		Namespace: namespace,
	}, pipelineSource); err != nil {
		return nil, fmt.Errorf("failed to get pipeline source: %w", err)
	}

	return pipelineSource, nil
}

// getCredentials retrieves S3 credentials for the pipelineSource bucket secret, if configured.
func (r *PipelineInstanceReconciler) getCredentials(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipelineSource *pipelinesv1alpha1.PipelineSource) (string, string, error) {
	if pipelineSource.Spec.Bucket == nil {
		return "", "", nil
	}

	secretRef := pipelineSource.Spec.Bucket.CredentialsSecret
	if secretRef == nil {
		return "", "", nil
	}

	namespace := secretRef.Namespace
	if namespace == "" {
		namespace = pipelineInstance.Namespace
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

// listBucketFiles lists objects available to process for the pipelineSource configuration.
func (r *PipelineInstanceReconciler) listBucketFiles(ctx context.Context, pipelineSource *pipelinesv1alpha1.PipelineSource, accessKey, secretKey string) ([]string, error) {
	if pipelineSource.Spec.Bucket == nil {
		return nil, fmt.Errorf("pipelineSource has no bucket source configured")
	}

	bucket := pipelineSource.Spec.Bucket

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

// getPipeline retrieves the Pipeline resource referenced by the PipelineInstance
func (r *PipelineInstanceReconciler) getPipeline(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) (*pipelinesv1alpha1.Pipeline, error) {
	namespace := pipelineInstance.Namespace
	if pipelineInstance.Spec.PipelineRef.Namespace != nil {
		namespace = *pipelineInstance.Spec.PipelineRef.Namespace
	}

	pipeline := &pipelinesv1alpha1.Pipeline{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      pipelineInstance.Spec.PipelineRef.Name,
		Namespace: namespace,
	}, pipeline); err != nil {
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}

	return pipeline, nil
}

// setCondition sets or updates a condition in the PipelineInstance status
func (r *PipelineInstanceReconciler) setCondition(pipelineInstance *pipelinesv1alpha1.PipelineInstance, conditionType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: pipelineInstance.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}

	meta.SetStatusCondition(&pipelineInstance.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pipelinesv1alpha1.PipelineInstance{}).
		Owns(&batchv1.Job{}).
		Owns(&appsv1.Deployment{}).
		Named("pipelineinstance").
		Complete(r)
}
