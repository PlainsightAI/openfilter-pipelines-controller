# Release notes

## v0.9.0

### Upgrade action required for bare-Helm installs

**This release changes the `pipelineinstances` CRD** (`status.executionStartTime`, added by #117). Helm installs
a chart's CRDs once on first install and never updates them on `helm upgrade` — see the caveat documented in
v0.8.0's `NOTES.txt` (PLAT-1572 — #119). ArgoCD-managed installs pick this CRD change up automatically as part of
their normal sync. Bare-Helm installs must re-apply the CRDs manually after upgrading, or the new field is silently
pruned by the API server and the restart-safe execution-timeout enforcement below will not work:

```bash
helm show crds oci://ghcr.io/plainsightai/charts/openfilter-pipelines-controller \
  --version 0.9.0 | kubectl apply -f -
```

### Fixed

- **Batch Pending/Unschedulable status reporting (PLAT-1597 — #120)**: batch `PipelineInstance`s no longer become `Progressing`/`Processing` merely because a Job exists. They remain `Starting` until a pod, init container, or equivalent confirmed execution evidence has actually started, unless the pod list can't be read, in which case the controller fails open to `Processing`. Running or completed init containers count as execution evidence, and `ExecutionStartTime` is not stamped on the pod-list fail-open path.

### Added

- **Durable batch execution-start anchor (PLAT-1570 — #117)**: `PipelineInstance.status.executionStartTime` records the first confirmed execution transition, providing the durable controller-side anchor needed for restart-safe `ExecutionTimeoutSeconds` enforcement by the deployment agent. This is the CRD change called out above.

### Changed

- Update the bundled OpenFilter builtin image pins (video-in, image-out, webvis) to `1.3.0` (#114, #115, #116).

### Operations / Documentation

- **Bare Helm CRD upgrade guidance (PLAT-1572 — #119)**: documents that Helm does not automatically upgrade CRDs and provides the OCI-compatible manual CRD apply procedure for non-ArgoCD installs.

## v0.8.0

### Added

- **Batch runs preserve per-source-file identity**: the claimer writes each downloaded object's real source URI (`s3://bucket/key`) to a `<SOURCE_PATH>.source_uri` sidecar, and the controller wires `FILTER_OVERRIDE_SOURCE_URI_FILE` into every entry filter, so the pipeline reports the true source file as `meta['src']` even though the media is downloaded to a fixed generic path. This lets downstream event data be attributed per source file for multi-file folder batches.
- **GKE device-plugin GPU sharing for multi-model pipelines (PLAT-1496 — #111)**: on managed clusters (GKE) whose GPU stack ignores `NVIDIA_VISIBLE_DEVICES`, a multi-model pipeline pod now gets CUDA on *every* model stage instead of only the lead. A new `GPU_SHARING_STRATEGY` flag / env (`time-sharing` | `mps`; empty keeps the on-prem `NVIDIA_VISIBLE_DEVICES=all` + RuntimeClass default) switches the reconciler into a device-plugin mode where each GPU container requests `nvidia.com/gpu` (limit == request, extended-resource compliant) and the pod carries `cloud.google.com/gke-gpu-sharing-strategy` + `gke-max-shared-clients-per-gpu` (= the pod's GPU-container count), so GKE node auto-provisioning packs the stages onto one physical GPU with no static node pool. Wired into all three pod builders (streaming, batch, multi-source batch); single-GPU-container pods stay exclusive, and the on-prem env-share path is unchanged.
- **Startup validation of `GPU_SHARING_STRATEGY`**: any value other than `""` / `time-sharing` / `mps` fails the controller fast at boot, instead of being stamped onto the pod nodeSelector and leaving pods permanently `Pending` on GKE.

### Changed

- **New `sourcePath` field (default `/ws/input`, extension-less), exposed to filters as `SOURCE_PATH`; `videoInputPath` is now a deprecated alias** kept for backward compatibility and removed at 1.0. The old `/ws/input.mp4` default forced a `.mp4` extension onto every downloaded object, which silently broke the extension-sensitive `image-in` filter in the batch queue path (images written to `input.mp4` were skipped). Entry filters are now extension-agnostic, so a generic name is correct; authors can reference `file://$(SOURCE_PATH)` or the legacy `file://$(VIDEO_INPUT_PATH)`. Pipelines that set `sourcePath` (or the deprecated `videoInputPath`) explicitly are unaffected.

## v0.7.1

### Changed

- Update the bundled OpenFilter builtin image pins (video-in, image-out, webvis) to `1.2.2` (#107, #108, #109). Pipelines now render on the apt-upgraded 1.2.2 builtins, clearing the OS-package + pip-bundled ffmpeg CVEs the older pins carried.

### Added

- SBOM attestation + shift-left security scan for the released controller image (#105).

## v0.7.0

### Added

- **Filter image volumes (PLAT-1094, PLAT-1095, PLAT-1096, PLAT-1097 — #88, #89, #90, #91)**: filters can mount OCI images (trained models, BYO weights) as read-only volumes via the Kubernetes `image` volume source. `spec.filters[].imageVolumes[]` on the Pipeline CRD (`FilterImageVolume`: `name`/`image`/`mountPath`, optional `pullPolicy`/`subPath`/`pullSecret`); rendering in all three pod builders (batch, streaming, multi-source batch) with pod-level dedup by `(image, pullSecret)`, content-addressed volume names, and pull-secret merge into `imagePullSecrets`; a startup cluster-version probe that terminally rejects image-volume pipelines on clusters below 1.35 (`Degraded=True/UnsupportedClusterVersion`, Warning Event, no requeue) — the ImageVolume feature gate is off by default through 1.34; sample manifest and Kind e2e coverage. Pipelines without `imageVolumes` render byte-identical to before, so the feature is opt-in purely through the CRD field.
- **`runtimeClassName` on GPU pipeline pods (PLAT-1272 — #87)**: opt-in setting that stamps a RuntimeClass onto pods requesting `nvidia.com/gpu`, for clusters where the NVIDIA runtime is not the containerd default (e.g. k3s) — without it the device is allocated but driver/CUDA injection never happens and the filter crashes at startup.

### Fixed

- **Batch-run status surfaces the claimer/filter's real error (PLAT-1353 — #94)**: a failed batch run's status now carries the failing container's actual error (e.g. `NoSuchBucket`, auth/signature failures, OOMKill) instead of only the Job-level `Job has reached the specified backoff limit` message — previously the real cause lived in a short-lived container that was deleted on failure, making runtime failures undiagnosable from status.
- **`services` RBAC in the kustomize deploy path (#92)**: the generated role was missing grants the controller needs, causing silent `Forbidden` errors on kustomize installs (chart installs were unaffected).

### Changed

- **Helm chart manager ClusterRole is generated, with a CI parity gate (PLAT-1352 — #95)**: `make helm-update-rbac` splices the kubebuilder-generated rules into the chart between markers, and CI fails on drift between `config/rbac/` and the chart. The sync also removed five chart-only grants the controller never used (pods create/patch/update, pods/status watch, secrets patch) — a least-privilege tightening applied automatically on upgrade.
- **Environment overrides moved to the private gitops repo (#84, #86)**: `deployment/**/overrides/` and `values-production.yaml` are gone from this repo; per-environment configuration lives in gitops.
- Demo filter pins bumped to openfilter 1.1.2 (`video-in`, `webvis`, `image-out` — #80, #81, #82).

### Dependencies / CI

- `google.golang.org/grpc` 1.79.3 → 1.82.1 (#97), `github.com/google/cel-go` 0.26.0 → 0.29.0 (#96), `golang.org/x/crypto` 0.51.0 → 0.52.0 (#85), `golang.org/x/net` 0.48.0 → 0.55.0 (#83).
- CI actions moved to Node 24 runtimes (checkout v6, setup-go v6, golangci-lint-action v9, upload/download-artifact v7/v8 — #78, #79).

## v0.6.2 and earlier

Releases up to and including v0.6.2 predate this file; see the [GitHub Releases](https://github.com/PlainsightAI/openfilter-pipelines-controller/releases) page and PR #77/#76 for the 0.6.x release-pipeline reconnection.
