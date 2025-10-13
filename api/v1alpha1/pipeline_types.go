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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// SecretReference contains information to locate a Kubernetes Secret
type SecretReference struct {
	// name is the name of the secret
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// namespace is the namespace of the secret
	// If empty, uses the same namespace as the Pipeline resource
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// ObjectStorageSource defines an S3-compatible object storage bucket
type ObjectStorageSource struct {
	// bucket is the name of the S3-compatible bucket
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// prefix is an optional path prefix within the bucket (e.g., "input-data/")
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// endpoint is the S3-compatible endpoint URL (required for non-AWS S3)
	// Examples:
	//   - MinIO: "http://minio.example.com:9000"
	//   - GCS: "https://storage.googleapis.com"
	//   - Custom S3: "https://s3.custom.example.com"
	// Leave empty for AWS S3 (will use default AWS endpoints)
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// region is the bucket region (e.g., "us-east-1")
	// Required for AWS S3, optional for other providers
	// +optional
	Region string `json:"region,omitempty"`

	// credentialsSecret references a Secret containing access credentials
	// Expected keys: "accessKeyId" and "secretAccessKey"
	// +optional
	CredentialsSecret *SecretReference `json:"credentialsSecret,omitempty"`

	// insecureSkipTLSVerify skips TLS certificate verification (useful for dev/test)
	// +optional
	// +kubebuilder:default=false
	InsecureSkipTLSVerify bool `json:"insecureSkipTLSVerify,omitempty"`

	// usePathStyle forces path-style addressing (endpoint.com/bucket vs bucket.endpoint.com)
	// Required for MinIO and some S3-compatible services
	// +optional
	// +kubebuilder:default=false
	UsePathStyle bool `json:"usePathStyle,omitempty"`
}

// Filter defines a containerized processing step in the pipeline
type Filter struct {
	// name is a unique identifier for this filter within the pipeline
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// image is the container image to run (e.g., "myregistry/filter:v1.0")
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// env is a list of environment variables to set in the container
	// Uses the standard Kubernetes EnvVar type for full compatibility
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// args are the command arguments to pass to the container
	// +optional
	Args []string `json:"args,omitempty"`

	// command overrides the default entrypoint of the container
	// +optional
	Command []string `json:"command,omitempty"`

	// resources defines compute resource requirements for this filter
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// imagePullPolicy determines when to pull the container image
	// +optional
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
}

// ExecutionConfig defines execution parameters for pipeline runs
type ExecutionConfig struct {
	// parallelism defines the maximum number of parallel executions (max concurrent pods)
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	Parallelism *int32 `json:"parallelism,omitempty"`

	// maxAttempts defines the maximum number of retry attempts per file
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	MaxAttempts *int32 `json:"maxAttempts,omitempty"`

	// pendingTimeout defines the time after which pending messages are reclaimed
	// Supports Kubernetes duration format (e.g., "15m", "1h", "30s")
	// +optional
	// +kubebuilder:default="15m"
	PendingTimeout *metav1.Duration `json:"pendingTimeout,omitempty"`
}

// PipelineSpec defines the desired state of Pipeline
type PipelineSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// input defines the S3-compatible bucket containing input data for the pipeline
	// +kubebuilder:validation:Required
	Input ObjectStorageSource `json:"input"`

	// filters is an ordered list of processing steps to apply to the input data
	// Filters are executed sequentially in the order they are defined
	// +optional
	// +kubebuilder:validation:MinItems=1
	Filters []Filter `json:"filters,omitempty"`

	// execution defines default execution parameters for pipeline runs
	// These values can be overridden by individual PipelineRuns
	// +optional
	Execution *ExecutionConfig `json:"execution,omitempty"`
}

// PipelineStatus defines the observed state of Pipeline.
type PipelineStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the Pipeline resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Pipeline is the Schema for the pipelines API
type Pipeline struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of Pipeline
	// +required
	Spec PipelineSpec `json:"spec"`

	// status defines the observed state of Pipeline
	// +optional
	Status PipelineStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// PipelineList contains a list of Pipeline
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Pipeline{}, &PipelineList{})
}
