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
	"crypto/sha256"
	"encoding/hex"

	corev1 "k8s.io/api/core/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// applyImageVolumes materializes every filter's imageVolumes (PLAT-1095) onto
// an assembled pod spec: one pod-level image-source volume per distinct
// (image, pullSecret) pair — filters sharing a model image share the pod
// volume and the node-level layer cache — a read-only VolumeMount on each
// declaring filter's container, and each volume's pullSecret merged into the
// pod's imagePullSecrets. A pipeline without imageVolumes is a complete
// no-op, so existing pipelines render byte-identical.
//
// The image volume source requires Kubernetes 1.33+; admission gating for
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
