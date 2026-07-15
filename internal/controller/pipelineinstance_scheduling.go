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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// nvidiaVisibleDevicesEnvName is the env var name read by the NVIDIA container
// runtime to scope visible GPU devices. Setting it to "all" makes every GPU
// allocated to the pod available to the container; this is the mechanism that
// lets multiple GPU-using containers in the same pod share the allocated
// device(s) without each declaring nvidia.com/gpu in its Resources.
const nvidiaVisibleDevicesEnvName = "NVIDIA_VISIBLE_DEVICES"

// defaultGPUCount is the per-instance GPU count used by the reconciler when
// PipelineInstanceSpec does not carry an explicit count. PLAT-1113 (the API PR)
// will add a ResourceConfig.GPUCount field; until then the reconciler defaults
// to 1, matching the previous behavior where each per-filter
// `nvidia.com/gpu: 1` declaration implicitly asked for a single device.
const defaultGPUCount int64 = 1

// mergeNodeSelector builds the effective NodeSelector for a pipeline pod by
// merging the controller-wide GPU labels (only when the pipeline requires GPU)
// with the per-instance NodeSelector. Instance values win on key conflict.
//
// The returned map is always a fresh allocation so the caller can mutate it
// without affecting the reconciler's shared GPUNodeSelectorLabels. Returns
// nil when neither map contributes any keys, to preserve the existing
// nil-vs-empty-map behavior that pre-PLAT-1109 tests rely on.
func mergeNodeSelector(gpuLabels map[string]string, instanceLabels map[string]string, pipelineRequiresGPU bool) map[string]string {
	var out map[string]string
	if len(gpuLabels) > 0 && pipelineRequiresGPU {
		out = make(map[string]string, len(gpuLabels)+len(instanceLabels))
		for k, v := range gpuLabels {
			out[k] = v
		}
	}
	for k, v := range instanceLabels {
		if out == nil {
			out = make(map[string]string, len(instanceLabels))
		}
		out[k] = v // instance wins on conflict
	}
	return out
}

// applyInstanceScheduling layers the per-instance scheduling fields
// (Tolerations, Affinity) onto a PodSpec that already has its container set,
// node selector, and any controller-injected tolerations applied. Tolerations
// are appended (never replaced); Affinity is a pure passthrough.
//
// Calling this with a nil spec is a no-op for the caller's convenience.
func applyInstanceScheduling(podSpec *corev1.PodSpec, instanceSpec pipelinesv1alpha1.PipelineInstanceSpec) {
	if podSpec == nil {
		return
	}
	if len(instanceSpec.Tolerations) > 0 {
		podSpec.Tolerations = append(podSpec.Tolerations, instanceSpec.Tolerations...)
	}
	if instanceSpec.Affinity != nil {
		podSpec.Affinity = instanceSpec.Affinity
	}
}

// applyGPUContainerSharing rewrites the container slice in place to implement
// the "single container holds the nvidia.com/gpu limit, others share via env"
// pattern.
//
// For each GPU-requesting container (per containerResourcesRequireGPU):
//   - injects NVIDIA_VISIBLE_DEVICES=all into the container env so the NVIDIA
//     runtime exposes the allocated device(s) to the container, idempotent
//     against an existing identical entry.
//   - on the FIRST GPU container (in declaration order), sets the
//     nvidia.com/gpu limit to gpuCount (defaulting to 1) so kubelet asks the
//     device plugin for that many devices.
//   - on subsequent GPU containers, strips any nvidia.com/gpu key from both
//     Resources.Limits and Resources.Requests. This avoids the
//     sum-of-containers trap where two filters each declaring
//     `nvidia.com/gpu: 1` silently doubles the pod's hardware footprint —
//     the desired count is per-pod, declared once.
//
// CPU-only containers are untouched. This function makes no assumptions about
// nodeSelector / tolerations / affinity — those are handled separately.
//
// Ambiguity note: if a non-first RequiresGPU container ALSO declared a per-
// filter `nvidia.com/gpu` limit, that declaration is intentionally dropped
// here. The per-instance count is authoritative; per-filter GPU resource
// declarations beyond the first are treated as redundant signal, not as an
// additive request.
func applyGPUContainerSharing(containers []corev1.Container, gpuCount int64) {
	if gpuCount <= 0 {
		gpuCount = defaultGPUCount
	}
	seenLead := false
	for i := range containers {
		if !containerResourcesRequireGPU(containers[i].Resources) {
			continue
		}
		injectNvidiaVisibleDevices(&containers[i])
		if !seenLead {
			// Stamp the canonical count on the lead container. Overwrite any
			// pre-existing limit so the per-instance count is the single
			// source of truth.
			if containers[i].Resources.Limits == nil {
				containers[i].Resources.Limits = corev1.ResourceList{}
			}
			containers[i].Resources.Limits["nvidia.com/gpu"] = *resource.NewQuantity(gpuCount, resource.DecimalSI)
			seenLead = true
			continue
		}
		// Non-lead GPU container: strip nvidia.com/gpu from both Limits and
		// Requests. The container still sees the device via NVIDIA_VISIBLE_DEVICES.
		delete(containers[i].Resources.Limits, "nvidia.com/gpu")
		delete(containers[i].Resources.Requests, "nvidia.com/gpu")
	}
}

// injectNvidiaVisibleDevices appends NVIDIA_VISIBLE_DEVICES=all to the
// container's env if it is not already present. Idempotent so callers can
// invoke it without checking first.
func injectNvidiaVisibleDevices(container *corev1.Container) {
	for _, e := range container.Env {
		if e.Name == nvidiaVisibleDevicesEnvName {
			return
		}
	}
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  nvidiaVisibleDevicesEnvName,
		Value: "all",
	})
}

// instanceGPUCount returns the GPU count to stamp on the lead GPU container.
//
// TODO(PLAT-1113): once PipelineInstanceSpec carries the per-instance count
// (via ResourceConfig.GPUCount in the API), read it here. For now we always
// return the package-level default. Keeping this as a function (rather than
// inlining the constant) makes the future wiring a one-line change.
func instanceGPUCount(_ pipelinesv1alpha1.PipelineInstanceSpec) int64 {
	return defaultGPUCount
}

// gpuRuntimeClassName returns the RuntimeClass name to set on a pipeline pod,
// but only when the pod requires a GPU and a name is configured; nil otherwise
// (leaving RuntimeClassName unset, i.e. the cluster's default runtime).
//
// The device plugin allocates nvidia.com/gpu, but on clusters where the nvidia
// runtime is not the default containerd runtime the driver/CUDA libraries are
// only injected when the pod runs under a RuntimeClass that selects the nvidia
// runtime (k3s, for example, auto-creates an `nvidia` RuntimeClass). Setting it
// here removes the need to make nvidia the node-wide default runtime.
//
// This is opt-in and a no-op by default (empty name): on managed platforms where
// the driver is injected for you, leave it unset. It matters for self-managed
// clusters (k3s, kubeadm, bare EKS) where runc is the default runtime and the
// operator cannot or should not flip the node-wide default. RuntimeClass is the
// Kubernetes-idiomatic, GPU-pod-scoped alternative to that node-wide change.
func gpuRuntimeClassName(name string, pipelineRequiresGPU bool) *string {
	if name == "" || !pipelineRequiresGPU {
		return nil
	}
	rc := name
	return &rc
}
