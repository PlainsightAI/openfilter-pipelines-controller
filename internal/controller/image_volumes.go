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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	k8sversion "k8s.io/apimachinery/pkg/version"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// minImageVolumeVersion is the first Kubernetes release that serves the image
// volume source by default. The source is alpha from 1.31 and beta from 1.33,
// but the ImageVolume feature gate stays Default:false through 1.34 (upstream
// kube_features.go); 1.35 is the first release with the gate on by default
// (GA 1.36).
var minImageVolumeVersion = utilversion.MustParseGeneric("1.35.0")

// ImageVolumeSupportReason returns a non-empty operator-facing reason when the
// given API server version cannot serve image volume sources, and "" when it
// can (PLAT-1096). Nil or unparseable versions return "" — fail-open:
// pipelines without imageVolumes are unaffected either way, and a cluster we
// cannot classify should not block ones that might work. 1.31-1.34 clusters
// with the ImageVolume feature gate explicitly enabled are still rejected —
// the gate is not observable from here, which is why the message names it.
func ImageVolumeSupportReason(info *k8sversion.Info) string {
	if info == nil {
		return ""
	}
	v, err := utilversion.ParseGeneric(info.GitVersion)
	if err != nil {
		return ""
	}
	if v.AtLeast(minImageVolumeVersion) {
		return ""
	}
	return fmt.Sprintf(
		"the cluster does not serve the image volume source by default: Kubernetes %s+ required (the ImageVolume feature gate exists from 1.31 but is off by default through 1.34), detected server version %s",
		minImageVolumeVersion, info.GitVersion)
}

// pipelineHasImageVolumes reports whether any filter declares imageVolumes.
func pipelineHasImageVolumes(pipeline *pipelinesv1alpha1.Pipeline) bool {
	for _, f := range pipeline.Spec.Filters {
		if len(f.ImageVolumes) > 0 {
			return true
		}
	}
	return false
}

// rejectUnsupportedImageVolumes fails a PipelineInstance whose Pipeline
// declares filter imageVolumes on a cluster that cannot serve the image
// volume source (PLAT-1096). Without this, an older API server silently
// prunes the unknown volume-source field and the failure surfaces as an
// opaque pod-admission error or a filter crashing on a missing model.
// Returns done=true when the caller must return the accompanying
// result/error instead of continuing the reconcile.
func (r *PipelineInstanceReconciler) rejectUnsupportedImageVolumes(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline) (bool, ctrl.Result, error) {
	if r.ImageVolumesUnsupportedReason == "" || !pipelineHasImageVolumes(pipeline) {
		return false, ctrl.Result{}, nil
	}
	log := logf.FromContext(ctx)
	msg := fmt.Sprintf("pipeline %q declares filter imageVolumes but %s", pipeline.Name, r.ImageVolumesUnsupportedReason)
	log.Info("Rejecting image-volume pipeline on unsupported cluster", "pipeline", pipeline.Name)
	if r.Recorder != nil {
		r.Recorder.Event(pipelineInstance, corev1.EventTypeWarning, ReasonUnsupportedClusterVersion, msg)
	}
	r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, ReasonUnsupportedClusterVersion, msg)
	r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, ReasonUnsupportedClusterVersion, msg)
	if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
		if apierrors.IsConflict(statusErr) {
			return true, ctrl.Result{Requeue: true}, nil
		}
		log.Error(statusErr, "Failed to update status after image-volume cluster-support rejection")
		return true, ctrl.Result{}, statusErr
	}
	return true, ctrl.Result{}, nil
}

// applyImageVolumes materializes every filter's imageVolumes (PLAT-1095) onto
// an assembled pod spec: one pod-level image-source volume per distinct
// (image, pullSecret) pair — filters sharing a model image share the pod
// volume and the node-level layer cache — a read-only VolumeMount on each
// declaring filter's container, and each volume's pullSecret merged into the
// pod's imagePullSecrets. A pipeline without imageVolumes is a complete
// no-op, so existing pipelines render byte-identical.
//
// The image volume source requires Kubernetes 1.35+ by default; admission gating for
// older clusters is PLAT-1096. Filters dedup on (image, pullSecret) only, so
// the first declaration's pullPolicy wins for a shared volume.
func applyImageVolumes(podSpec *corev1.PodSpec, filters []pipelinesv1alpha1.Filter) {
	volumeNameByKey := make(map[string]string)
	for _, filter := range filters {
		if len(filter.ImageVolumes) == 0 {
			continue
		}
		container := containerByName(podSpec, filter.Name)
		if container == nil {
			continue
		}
		for _, vol := range filter.ImageVolumes {
			key := vol.Image + "\x00" + vol.PullSecret
			name, ok := volumeNameByKey[key]
			if !ok {
				name = imageVolumeName(vol.Image, vol.PullSecret)
				volumeNameByKey[key] = name
				podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
					Name: name,
					VolumeSource: corev1.VolumeSource{
						Image: &corev1.ImageVolumeSource{
							Reference:  vol.Image,
							PullPolicy: vol.PullPolicy,
						},
					},
				})
				if vol.PullSecret != "" {
					podSpec.ImagePullSecrets = appendPullSecret(podSpec.ImagePullSecrets, vol.PullSecret)
				}
			}
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      name,
				MountPath: vol.MountPath,
				SubPath:   vol.SubPath,
				ReadOnly:  true,
			})
		}
	}
}

// imageVolumeName derives a stable DNS-1123 pod-volume name from the dedup
// key. Content-addressed so reordering filters or volume entries never
// renames the pod-level volume.
func imageVolumeName(image, pullSecret string) string {
	sum := sha256.Sum256([]byte(image + "\x00" + pullSecret))
	return "imgvol-" + hex.EncodeToString(sum[:])[:10]
}

func containerByName(podSpec *corev1.PodSpec, name string) *corev1.Container {
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == name {
			return &podSpec.Containers[i]
		}
	}
	return nil
}

// appendPullSecret returns secrets with name appended unless already present.
// The pod spec aliases pipeline.Spec.ImagePullSecrets, so the append always
// goes through a fresh slice — never the caller's backing array — to avoid
// mutating the shared Pipeline object.
func appendPullSecret(secrets []corev1.LocalObjectReference, name string) []corev1.LocalObjectReference {
	for _, s := range secrets {
		if s.Name == name {
			return secrets
		}
	}
	out := make([]corev1.LocalObjectReference, 0, len(secrets)+1)
	out = append(out, secrets...)
	return append(out, corev1.LocalObjectReference{Name: name})
}
