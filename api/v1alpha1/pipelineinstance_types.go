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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExecutionConfig defines execution parameters for pipeline instances
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

// PipelineInstanceSpec defines the desired state of PipelineInstance
type PipelineInstanceSpec struct {
	// pipelineRef references the Pipeline resource to execute
	// +required
	PipelineRef PipelineReference `json:"pipelineRef"`

	// sourceRef references the PipelineSource resource for input configuration
	// +required
	SourceRef SourceReference `json:"sourceRef"`

	// execution defines how the pipeline should be executed
	// +optional
	Execution *ExecutionConfig `json:"execution,omitempty"`
}

// PipelineReference defines a reference to a Pipeline resource
type PipelineReference struct {
	// name is the name of the Pipeline resource
	// +required
	Name string `json:"name"`

	// namespace is the namespace of the Pipeline resource
	// If not specified, the PipelineInstance's namespace is used
	// +optional
	Namespace *string `json:"namespace,omitempty"`
}

// SourceReference defines a reference to a PipelineSource resource
type SourceReference struct {
	// name is the name of the PipelineSource resource
	// +required
	Name string `json:"name"`

	// namespace is the namespace of the PipelineSource resource
	// If not specified, the PipelineInstance's namespace is used
	// +optional
	Namespace *string `json:"namespace,omitempty"`
}

// PipelineInstanceStatus defines the observed state of PipelineInstance.
type PipelineInstanceStatus struct {
	// counts tracks the number of files in each state (Batch mode only)
	// +optional
	Counts *FileCounts `json:"counts,omitempty"`

	// jobName is the name of the Kubernetes Job created for this instance (Batch mode only)
	// +optional
	JobName string `json:"jobName,omitempty"`

	// streaming provides observability into streaming pipeline instances (Stream mode only)
	// +optional
	Streaming *StreamingStatus `json:"streaming,omitempty"`

	// startTime is when the pipeline instance was started
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// completionTime is when the pipeline instance completed
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// conditions represent the current state of the PipelineInstance resource.
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

// FileCounts tracks the number of files in each processing state
type FileCounts struct {
	// totalFiles is the total number of files to process
	// +optional
	TotalFiles int64 `json:"totalFiles,omitempty"`

	// queued is the number of files waiting to be processed
	// +optional
	Queued int64 `json:"queued,omitempty"`

	// running is the number of files currently being processed
	// +optional
	Running int64 `json:"running,omitempty"`

	// succeeded is the number of files successfully processed
	// +optional
	Succeeded int64 `json:"succeeded,omitempty"`

	// failed is the number of files that failed processing
	// +optional
	Failed int64 `json:"failed,omitempty"`
}

// StreamingStatus provides observability into streaming pipeline instances
type StreamingStatus struct {
	// readyReplicas is the number of ready replicas in the streaming deployment
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// updatedReplicas is the number of updated replicas in the streaming deployment
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// availableReplicas is the number of available replicas in the streaming deployment
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

	// containerRestarts is the aggregate count of container restarts across all pods
	// +optional
	ContainerRestarts int32 `json:"containerRestarts,omitempty"`

	// lastReadyTime is the last time the deployment became ready
	// +optional
	LastReadyTime *metav1.Time `json:"lastReadyTime,omitempty"`

	// lastFrameAt is the timestamp of the last processed frame (best-effort)
	// +optional
	LastFrameAt *metav1.Time `json:"lastFrameAt,omitempty"`

	// deploymentName is the name of the Kubernetes Deployment created for this streaming instance
	// +optional
	DeploymentName string `json:"deploymentName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// PipelineInstance is the Schema for the pipelineinstances API
// It represents an execution of a Pipeline with a specific PipelineSource
type PipelineInstance struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of PipelineInstance
	// +required
	Spec PipelineInstanceSpec `json:"spec"`

	// status defines the observed state of PipelineInstance
	// +optional
	Status PipelineInstanceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelineInstanceList contains a list of PipelineInstance
type PipelineInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelineInstance `json:"items"`
}

// GetQueueStream returns the Valkey stream key for this PipelineInstance
// Format: pi:<uid>:work
func (pi *PipelineInstance) GetQueueStream() string {
	return "pi:" + string(pi.UID) + ":work"
}

// GetQueueGroup returns the Valkey consumer group name for this PipelineInstance
// Format: cg:<uid>
func (pi *PipelineInstance) GetQueueGroup() string {
	return "cg:" + string(pi.UID)
}

// GetInstanceID returns the instance ID (UID) for this PipelineInstance
func (pi *PipelineInstance) GetInstanceID() string {
	return string(pi.UID)
}

func init() {
	SchemeBuilder.Register(&PipelineInstance{}, &PipelineInstanceList{})
}
