# Streaming Mode Implementation Plan

This plan introduces a "streaming" execution mode where a Pipeline processes an RTSP stream continuously using a Kubernetes Deployment, while keeping the existing batch/S3 mode (Job + Valkey) unchanged. Both modes can coexist in the same controller without feature flags. Valkey remains required for batch.

## Summary
- Mode is defined on `Pipeline.spec.mode` (`Batch` default, `Stream` alternative).
- `Stream` mode uses a `Deployment` per `PipelineRun` with `replicas: 1`.
- RTSP source configuration lives in `Pipeline.spec.source.rtsp`.
- The controller injects `RTSP_URL` and (optional) credentials as env vars for filters to consume directly (e.g., the `video-in` filter sets `sources=$(RTSP_URL)` or a full `rtsp://username:password@host:port/path`). No ingest sidecar is required.
- Optional `idleTimeout` can complete and clean up a streaming run when the stream is idle for a configured duration.

## API Changes

### Pipeline (api/v1alpha1/pipeline_types.go)
- Add `spec.mode`:
  - Enum: `Batch | Stream`.
  - Default: `Batch`.
- Add `spec.source.rtsp` (mutually exclusive with `spec.source.bucket`):
  - `url` (string, required)
  - `credentialsSecret` (optional; keys: `username`, `password`)
  - `transport` (optional; enum: `tcp|udp|auto`, default `tcp`)
  - `idleTimeout` (optional `metav1.Duration`): continuous Unready ≥ idleTimeout → controller completes the run and deletes the Deployment.
- Validation (controller-level with CRD hints):
  - If `mode=Batch`: require `source.bucket`, forbid `source.rtsp`.
  - If `mode=Stream`: require `source.rtsp`, forbid `source.bucket`.

### PipelineRun (api/v1alpha1/pipelinerun_types.go)
- No new spec fields (no `replicas` knob).
- Optional `status.streaming` block for observability:
  - `readyReplicas`, `updatedReplicas`, `availableReplicas`
  - `containerRestarts` (aggregate), `lastReadyTime`, `lastFrameAt` (best-effort)

## Controller Changes (internal/controller/pipelinerun_controller.go)

### Mode Branching
- Fetch referenced Pipeline and branch by `pipeline.spec.mode`:
  - `Batch` → existing path (S3 list → Valkey XADD → Job). Unchanged.
  - `Stream` → streaming path below (no Valkey, no Job).

### Streaming Path
- `ensureDeployment(ctx, pr, pipeline)`:
  - Create/Update `apps/v1.Deployment` named `<pipelinerun-name>-deploy`.
  - `replicas: 1`, `strategy: RollingUpdate (maxUnavailable=0, maxSurge=1)`.
  - Pod template:
    - No dedicated ServiceAccount required; pods run with the namespace default. `restartPolicy: Always`.
    - Volumes: none required by the controller for streaming mode (filters may declare their own if needed).
    - Filter containers built from `pipeline.spec.filters` (unchanged). Controller injects `RTSP_URL`, and optionally `RTSP_USERNAME`/`RTSP_PASSWORD`, to all filter containers. The `video-in` filter should reference these (e.g., `sources=$(RTSP_URL)`), or embed the full RTSP URL directly in its config.
- Status and conditions:
  - `Progressing=True` until desired replicas ready; `Available=True` when `readyReplicas == 1`.
  - Aggregate pod/container restarts; populate `status.streaming`.
- Idle handling:
  - If `rtsp.idleTimeout` set and the streaming pod remains Unready (no ready containers) for ≥ idleTimeout, mark run Succeeded, set `completionTime`, and delete the Deployment.
- Finalizers and cleanup:
  - On `PipelineRun` deletion, delete owned Deployment and remove finalizer when gone.
- Watches/Ownership:
  - Own `Deployments`; watch their `ReplicaSets`/`Pods` for status updates (in addition to existing Jobs/Pods).

## RBAC
- Add apps/v1 permissions to controller role:
  - `deployments;replicasets: get, list, watch, create, update, patch, delete`.
- Keep existing `jobs`, `pods`, `pods/status`, `secrets` permissions.

## Manifests & Charts
- Regenerate CRDs and DeepCopy: `make manifests generate`.
- Sync Helm CRDs: `make helm-update-crds`.
- No feature flags; Valkey stays mandatory for batch mode.

## Samples
- `config/samples/pipeline_rtsp.yaml` (mode: Stream):
  - `spec.mode: Stream`
  - `spec.source.rtsp.url: rtsp://...`
  - Optional `credentialsSecret`
  - Filters set the `video-in` source to RTSP, e.g. `config: - name: sources value: $(RTSP_URL)` (or a full `rtsp://username:password@host:port/path`).
- `config/samples/pipelinerun_stream.yaml`: minimal `PipelineRun` referencing the Pipeline.

## Testing
- Unit tests:
  - CRD defaulting/validation for `Pipeline.spec.mode` and `source` exclusivity.
  - Deployment spec build: env injection from Secret, `replicas=1`, owner refs.
  - Status transitions: `Progressing` → `Available`; `Degraded` on repeated failures.
  - Idle timeout completion path.
- E2E (Kind):
  - Deploy MediaMTX RTSP server; publish short test stream via ffmpeg.
  - Apply RTSP Pipeline + PipelineRun, assert Deployment ready and stable.
  - If `idleTimeout` set, stop source and verify run completes and Deployment is removed.

## Observability
- Controller metrics (optional additions):
  - `stream_ready_replicas`, `stream_container_restarts`, `stream_run_status` gauge.
- Logging: stream connectivity (ready/unready), disconnects/reconnects, idle completions.

## Open Items To Confirm
- `idleTimeout` semantics: continuous Unready ≥ `idleTimeout` → complete run and delete Deployment — acceptable?
- No ingest sidecar (filters read RTSP directly). 
- RTSP credentials: separate `username`/`password` keys in Secret (preferred), not embedded in URL.

## Next Steps
1. Implement CRD changes (Pipeline.mode, Source.rtsp with optional idleTimeout).
2. `make manifests generate` and commit updated CRDs.
3. Update controller: streaming branch (ensureDeployment, status, idle handling, finalizer).
4. Add apps RBAC, run `make manifests` again.
5. Add samples and README/DESIGN docs.
6. Add unit tests; wire e2e with MediaMTX.
7. `make test` and `make test-e2e` locally; iterate.
