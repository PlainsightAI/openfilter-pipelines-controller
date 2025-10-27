# OpenFilter Pipelines Controller — Getting Started

This guide gets you from zero to a working deployment of the OpenFilter Pipelines Controller, covers both execution modes (batch and stream), shows the required inputs for each mode, and walks through end‑to‑end demos using the manifests in this repository. It also includes kubectl validation steps we executed while writing this guide.

Repository: github.com/PlainsightAI/openfilter-pipelines-controller

## Overview

- Custom Resources:
  - `Pipeline` (recipe): defines input source and ordered filter containers.
  - `PipelineRun` (execution): references a `Pipeline` and executes it.
- Modes (Pipeline.spec.mode):
  - `batch` (default): object-storage batch processing via a single Kubernetes Job; work distributed with Valkey Streams.
  - `stream`: real-time RTSP stream processing via a single-replica Kubernetes Deployment; optional Services expose ports from filters.

## Prerequisites

- A Kubernetes cluster and `kubectl` configured against it.
- Container registry access if you build your own controller image.
- Valkey reachable from the controller (required for starting the controller; used by batch mode). You can deploy a development Valkey via `config/testing`.
- Optional object storage for batch pipelines (MinIO, AWS S3, GCS S3-compat, Azure blob S3-compat, etc.).
- Optional RTSP source for streaming mode (or use the demo RTSP server under `hack/rtsp-stream`).

Versions (repo defaults):
- Go 1.25.1, controller-runtime v0.22.1, Kubebuilder v4.9.0.

## Install the Controller (Helm)

Use the Helm chart under `charts/openfilter-pipelines-controller`.

```bash
# Install (set your own VALKEY endpoint)
helm install openfilter-pipelines-controller charts/openfilter-pipelines-controller \
  --namespace pipelines-system --create-namespace \
  --set controller.env.VALKEY_ADDR="<host:port>" \
  --set controller.env.VALKEY_PASSWORD=""

# With an in-cluster Valkey (enable the bundled subchart)
helm install openfilter-pipelines-controller charts/openfilter-pipelines-controller \
  --namespace pipelines-system --create-namespace \
  --set valkey.enabled=true
```

## Namespaces and ServiceAccount layout

- Batch mode: no special ServiceAccount is required for worker pods. The init “claimer” no longer patches pod annotations, and the controller infers ownership from Valkey using the consumer name (pod name). Jobs can run with the namespace’s default ServiceAccount.
- Stream mode: no special ServiceAccount is required. Streaming Deployments run with the namespace’s default ServiceAccount.

## Modes and Inputs

These fields come from the CRD types in `api/v1alpha1/` and the controller code under `internal/controller/`.

### Batch mode (spec.mode: batch)

- Purpose: process files from object storage using one Kubernetes Job.
- Work queue: Valkey Streams (required).
- Required Pipeline inputs:
  - `spec.source.bucket`:
    - `name` (string): bucket/container name.
    - Optional: `prefix` (string), `endpoint` (URL; use for MinIO/GCS/Azure S3-compat), `region`, `insecureSkipTLSVerify` (bool), `usePathStyle` (bool).
    - Optional `credentialsSecret`: Secret reference with keys `accessKeyId` and `secretAccessKey` (S3-compatible credentials).
  - `spec.filters[]`: ordered container steps. Each filter supports `image`, optional `command`, `args`, `env`, `resources`, and `config` (becomes `FILTER_<NAME>=<value>` envs).
  - Optional: `spec.videoInputPath` (default `/ws/input.mp4`), path where the init “claimer” stages the file.
- PipelineRun controls:
  - `spec.execution.parallelism` (default 10)
  - `spec.execution.maxAttempts` (default 3)
  - `spec.execution.pendingTimeout` (default 15m; reclaim stale work)

### Stream mode (spec.mode: stream)

- Purpose: process a live RTSP source via a single-replica Deployment.
- No Valkey queue; runs continuously (or until idle timeout fires).
- Required Pipeline inputs:
  - `spec.source.rtsp`:
    - `host` (string), `port` (default 554), `path` (string)
    - Optional `credentialsSecret`: Secret with keys `username` and `password` (controller injects `_RTSP_USERNAME/_RTSP_PASSWORD` and expands `RTSP_URL`).
    - Optional `idleTimeout` (duration): if the streaming pod remains Unready for this long, the controller marks the run complete and deletes the Deployment.
  - `spec.filters[]`: containers build the streaming pipeline; use `$(RTSP_URL)` in a filter config value to consume the injected URL.
  - Optional: `spec.services[]`: expose specific ports from filters as Services. Each entry has `name` (filter name), `port`, optional `targetPort` and `protocol`. Services are named `<pipelinerun-name>-<filter-name>-<index>`.
- PipelineRun: minimal; only references the Pipeline.

## Quickstarts

Below demos assume the controller is installed via Helm. Replace `<ns>` with your Helm release namespace (for example, `pipelines-system`).

### A. Batch mode demo (demo/pipeline_batch.yaml)

1) Create object storage credentials Secret in `<ns>` (example shows S3/MinIO style keys):

```bash
kubectl -n <ns> create secret generic gcs-credentials \
  --from-literal=accessKeyId="<ACCESS_KEY>" \
  --from-literal=secretAccessKey="<SECRET_KEY>"
```

2) Review and update `demo/pipeline_batch.yaml` with your bucket details, then apply:

```bash
kubectl -n <ns> apply -f demo/pipeline_batch.yaml
```

3) Start a run (uses `generateName`):

```bash
kubectl -n <ns> create -f demo/pipelinerun_batch.yaml
```

4) Observe progress:

```bash
# List PipelineRuns and watch status counts
kubectl -n <ns> get pipelineruns
kubectl -n <ns> describe pipelinerun <generated-name>

# List Job and pods created for the run
kubectl -n <ns> get job
kubectl -n <ns> get pods -l filter.plainsight.ai/pipelinerun=<generated-name>

# Tail a worker pod
kubectl -n <ns> logs -f <worker-pod-name>
```

5) Cleanup:

```bash
kubectl -n <ns> delete pipelinerun <generated-name>
kubectl -n <ns> delete pipeline pipeline-batch
```

### B. Stream mode demo with RTSP (demo/pipeline_rtsp.yaml)

You need an RTSP endpoint. The repo includes a MediaMTX-based demo server under `hack/rtsp-stream/`.

1) Deploy the RTSP demo server (defaults to a `rtsp-video-stream` Service on port 8554):

```bash
kubectl -n <ns> apply -f hack/rtsp-stream/                             # deployment/service
kubectl -n <ns> get pods -w                                             # wait Ready
```

2) Apply the streaming Pipeline and start a run:

```bash
kubectl -n <ns> apply -f demo/pipeline_rtsp.yaml
kubectl -n <ns> apply -f demo/pipelinerun_rtsp.yaml
```

3) Check Deployment status and optional Services:

```bash
# The controller creates a Deployment named <pipelinerun-name>-deploy
kubectl -n <ns> get deploy,pods -l pipelinerun=pipelinerun-rtsp

# If your Pipeline defines spec.services (e.g., webvis on 8080)
kubectl -n <ns> get svc
kubectl -n <ns> port-forward svc/pipelinerun-rtsp-webvis-0 8080:8080   # browse http://localhost:8080
```

4) Cleanup:

```bash
kubectl -n <ns> delete pipelinerun pipelinerun-rtsp
kubectl -n <ns> delete pipeline pipeline-rtsp
kubectl -n <ns> delete -f hack/rtsp-stream/
```

## Common kubectl snippets

```bash
# Pipelines and runs
kubectl -n <ns> get pipelines
kubectl -n <ns> get pipelineruns
kubectl -n <ns> describe pipelinerun <name>

# Batch: list pods by run
kubectl -n <ns> get pods -l filter.plainsight.ai/pipelinerun=<run-name>

# Stream: deployment/pods created for a run
kubectl -n <ns> get deploy,pods -l pipelinerun=<run-name>
```

## Troubleshooting

- Controller won’t start: set `VALKEY_ADDR` (and `VALKEY_PASSWORD` if needed) on the controller Deployment or use `config/testing` overlay.
- Batch run stays pending: ensure the Valkey Service is reachable from controller/pods; confirm credentials Secret for object storage exists and has keys `accessKeyId` and `secretAccessKey` in the Pipeline’s namespace.
- Workers no longer patch pods: the controller infers ownership via Valkey. No special ServiceAccount is required in workload namespaces.
- Streaming run never becomes Ready: verify RTSP `host:port`/`path` and credentials; check pod logs for connection errors.

## What we validated locally

We used Helm and kubectl to validate this guide:

```bash
# Lint the Helm chart — OK
helm lint charts/openfilter-pipelines-controller

# Render the chart — OK
helm template test charts/openfilter-pipelines-controller >/dev/null

# After installing the chart in your cluster, you can run the demos
# (these require CRDs to be present, which Helm installs from charts/.../crds)
kubectl -n <ns> apply -f demo/pipeline_batch.yaml
kubectl -n <ns> create -f demo/pipelinerun_batch.yaml
kubectl -n <ns> apply -f demo/pipeline_rtsp.yaml
kubectl -n <ns> apply -f demo/pipelinerun_rtsp.yaml
```

## File map (useful references)

- Types: `api/v1alpha1/pipeline_types.go`, `api/v1alpha1/pipelinerun_types.go`
- Controllers: `internal/controller/pipelinerun_controller_batch.go`, `internal/controller/pipelinerun_controller_streaming.go`
- Demo manifests: `demo/`
- Samples: `config/samples/`
- Testing overlay (controller + Valkey): `config/testing/`
- RBAC for worker pods: `config/rbac/pipeline_exec_*.yaml`
- RTSP demo server: `hack/rtsp-stream/`
