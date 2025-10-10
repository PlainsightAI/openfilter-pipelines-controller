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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// PipelineRunSpec defines the desired state of PipelineRun
type PipelineRunSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// pipelineRef references the Pipeline resource to execute
	// +required
	PipelineRef PipelineReference `json:"pipelineRef"`

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
	// If not specified, the PipelineRun's namespace is used
	// +optional
	Namespace *string `json:"namespace,omitempty"`
}

// ExecutionConfig defines execution parameters for the pipeline
type ExecutionConfig struct {
	// parallelism defines the maximum number of parallel executions
	// +optional
	// +kubebuilder:validation:Minimum=1
	Parallelism *int32 `json:"parallelism,omitempty"`

	// maxAttempts defines the maximum number of retry attempts
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxAttempts *int32 `json:"maxAttempts,omitempty"`
}

// PipelineRunStatus defines the observed state of PipelineRun.
type PipelineRunStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the PipelineRun resource.
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

// PipelineRun is the Schema for the pipelineruns API
type PipelineRun struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of PipelineRun
	// +required
	Spec PipelineRunSpec `json:"spec"`

	// status defines the observed state of PipelineRun
	// +optional
	Status PipelineRunStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// PipelineRunList contains a list of PipelineRun
type PipelineRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelineRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PipelineRun{}, &PipelineRunList{})
}
