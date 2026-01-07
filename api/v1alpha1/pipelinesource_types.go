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

// PipelineSourceSpec defines the desired state of PipelineSource
// Exactly one source type must be specified
type PipelineSourceSpec struct {
	// bucket defines an S3-compatible object storage source
	// Use this for Batch mode pipelines
	// +optional
	Bucket *BucketSource `json:"bucket,omitempty"`

	// rtsp defines an RTSP stream source
	// Use this for Stream mode pipelines
	// +optional
	RTSP *RTSPSource `json:"rtsp,omitempty"`
}

// PipelineSourceStatus defines the observed state of PipelineSource
type PipelineSourceStatus struct {
	// conditions represent the current state of the PipelineSource resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.bucket != null && "Bucket" || "RTSP"`,description="Source type"

// PipelineSource is the Schema for the pipelinesources API
// It defines an input source (S3 bucket or RTSP stream) that can be referenced by PipelineInstances
type PipelineSource struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the source configuration
	// +required
	Spec PipelineSourceSpec `json:"spec"`

	// status defines the observed state of PipelineSource
	// +optional
	Status PipelineSourceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelineSourceList contains a list of PipelineSource
type PipelineSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelineSource `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PipelineSource{}, &PipelineSourceList{})
}
