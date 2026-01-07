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

// BucketSource defines an S3-compatible object storage bucket source
type BucketSource struct {
	// name is the name of the S3-compatible bucket
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

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

// RTSPSource defines an RTSP stream source
type RTSPSource struct {
	// host is the RTSP server hostname or IP address
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`

	// port is the RTSP server port
	// +optional
	// +kubebuilder:default=554
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`

	// path is the RTSP stream path (e.g., "/stream1", "/live/camera1")
	// +optional
	Path string `json:"path,omitempty"`

	// credentialsSecret references a Secret containing RTSP credentials
	// Expected keys: "username" and "password"
	// +optional
	CredentialsSecret *SecretReference `json:"credentialsSecret,omitempty"`

	// idleTimeout defines the duration after which a continuously unready stream
	// will cause the PipelineRun to complete and the Deployment to be deleted.
	// If not set, the stream will run indefinitely.
	// +optional
	IdleTimeout *metav1.Duration `json:"idleTimeout,omitempty"`
}

// Source defines the input source for the pipeline
// Supports bucket (S3-compatible object storage) or rtsp (streaming video)
// Exactly one source type must be specified based on the pipeline mode
type Source struct {
	// bucket defines an S3-compatible object storage source
	// Required when mode is Batch, forbidden when mode is Stream
	// +optional
	Bucket *BucketSource `json:"bucket,omitempty"`

	// rtsp defines an RTSP stream source
	// Required when mode is Stream, forbidden when mode is Batch
	// +optional
	RTSP *RTSPSource `json:"rtsp,omitempty"`
}

// ConfigVar defines a configuration key-value pair that will be injected
// as an environment variable with the FILTER_ prefix
type ConfigVar struct {
	// name is the configuration key (will be prefixed with FILTER_ and uppercased)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// value is the configuration value
	// +kubebuilder:validation:Required
	Value string `json:"value"`
}

// PipelineMode defines the execution mode for a Pipeline
// +kubebuilder:validation:Enum=batch;stream
type PipelineMode string

const (
	// PipelineModeBatch runs the pipeline as a Kubernetes Job processing files from S3
	PipelineModeBatch PipelineMode = "batch"
	// PipelineModeStream runs the pipeline as a Kubernetes Deployment processing an RTSP stream
	PipelineModeStream PipelineMode = "stream"
)

// ServicePort defines a port to expose as a Kubernetes Service for a filter
type ServicePort struct {
	// name is the name of the filter to expose
	// Must match one of the filter names in the pipeline
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// port is the port number to expose
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`

	// targetPort is the port on the container to forward to
	// If not specified, defaults to the same value as port
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	TargetPort *int32 `json:"targetPort,omitempty"`

	// protocol is the network protocol for this port (TCP or UDP)
	// +optional
	// +kubebuilder:default=TCP
	// +kubebuilder:validation:Enum=TCP;UDP
	Protocol corev1.Protocol `json:"protocol,omitempty"`
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

	// config is a list of configuration key-value pairs that will be injected
	// as environment variables with the FILTER_ prefix.
	// For example, a config with name "sources" and value "mysource" will result
	// in the environment variable FILTER_SOURCES=mysource
	// +optional
	Config []ConfigVar `json:"config,omitempty"`

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

// PipelineSpec defines the desired state of Pipeline
type PipelineSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// mode defines the execution mode for this pipeline (batch or stream)
	// batch mode processes files from S3 using Kubernetes Jobs
	// stream mode processes RTSP streams using Kubernetes Deployments
	// +optional
	// +kubebuilder:default=batch
	Mode PipelineMode `json:"mode,omitempty"`

	// filters is an ordered list of processing steps to apply to the input data
	// Filters are executed sequentially in the order they are defined
	// +optional
	// +kubebuilder:validation:MinItems=1
	Filters []Filter `json:"filters,omitempty"`

	// services defines Kubernetes Services to expose filter ports
	// Only applies to Stream mode. Multiple services can expose different ports for the same filter.
	// Service naming: <pipelinerun-name>-<filter-name>-<index>
	// +optional
	Services []ServicePort `json:"services,omitempty"`

	// videoInputPath defines where the controller stores downloaded source files.
	// Downstream filters can reference this path to read the input artifact.
	// Defaults to /ws/input.mp4.
	// Only applies to Batch mode.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:default=/ws/input.mp4
	VideoInputPath string `json:"videoInputPath,omitempty"`
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
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of Pipeline
	// +required
	Spec PipelineSpec `json:"spec"`

	// status defines the observed state of Pipeline
	// +optional
	Status PipelineStatus `json:"status,omitempty"`
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
