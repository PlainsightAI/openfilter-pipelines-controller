# Release notes

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
