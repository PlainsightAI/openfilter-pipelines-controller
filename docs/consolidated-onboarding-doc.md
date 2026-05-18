# OpenFilter Pipelines Controller — Stream Mode E2E Demo

This runbook follows the **OpenFilter Pipelines Controller** docs. It walks
through a full end-to-end streaming demo using the repo manifests
(`demo/pipeline_rtsp.yaml`, `demo/pipelinesource_rtsp.yaml`,
`demo/pipelineinstance_rtsp.yaml`) and the demo RTSP server under
`hack/rtsp-stream/`.

> **Namespace:** every command below uses `<ns>` as a placeholder for your
> Helm release namespace. The recommended default (used in
> `GETTING_STARTED.md`) is `pipelines-system`. Substitute whatever value
> you passed to `helm install --namespace`.

**Doc concepts you are demonstrating**

- **Pipeline (recipe):** declares the ordered filter containers and any
  `services[]` it exposes. The Pipeline does **not** declare the source —
  sources are a separate CR.
- **PipelineSource (input):** declares the RTSP or bucket input that a run
  should consume (`PipelineSourceSpec` exposes `Bucket` and `RTSP` —
  `api/v1alpha1/pipelinesource_types.go`). Lives in its own CR so it can be
  reused across instances.
- **PipelineInstance (execution):** references a Pipeline and a
  PipelineSource (`spec.pipelineRef` + `spec.sourceRef`) and is what the
  controller reconciles into running workloads.
- **Stream mode:** real-time RTSP processing via a **single-replica
  Deployment** created by the controller. Optional Services expose filter
  ports.

---

## Prereqs (from docs)

- Controller installed in your cluster (Helm install already done).
- `kubectl` working against the cluster.
- Repo cloned locally: `openfilter-pipelines-controller`.

---

## Terminals you'll use

You will use **3 terminals**. Two of them must keep running during the demo.

### Terminal 1 — KEEP RUNNING

RTSP port-forward.

```bash
kubectl -n <ns> port-forward svc/rtsp-video-stream 8554:8554
```

**What this does:**
Creates a tunnel from your laptop → in-cluster RTSP demo Service so your
local RTSP publisher can push to it.

**Why the docs need it:**
The stream demo requires an RTSP endpoint. This makes the demo RTSP
server reachable from your machine.

---

### Terminal 2 — KEEP RUNNING

Publish a live test stream to `/stream`.

```bash
docker run --rm -it --platform linux/amd64 \
  jrottenberg/ffmpeg:6.1-alpine \
  -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000 \
  -rtsp_transport tcp \
  -c:v libx264 -tune zerolatency -pix_fmt yuv420p -preset veryfast \
  -c:a aac \
  -f rtsp \
  rtsp://host.docker.internal:8554/stream
```

> **Linux note:** `host.docker.internal` only resolves out of the box on
> Docker Desktop (macOS / Windows). On Linux with plain Docker Engine,
> add `--add-host=host.docker.internal:host-gateway` to the `docker run`
> invocation, or replace the URL with your host IP.

**What this does:**
Continuously publishes a color-bar test video into the RTSP demo server
at path `/stream`.

**Why the docs need it:**
Stream mode needs the PipelineSource's RTSP target to be a real, live
RTSP stream. This is your live input.

---

### Terminal 3 — Setup + validation

Use this for applying manifests and checking status / logs.

---

## Demo steps (E2E)

### 1) Deploy the demo RTSP server (in-cluster)

**Terminal 3:**

```bash
kubectl -n <ns> apply -f hack/rtsp-stream/
```

**What this does:**
Creates the demo RTSP Deployment + Service (`rtsp-video-stream`) inside
the cluster.

**Validate:**

```bash
kubectl -n <ns> get pods -l app=rtsp-video-stream
kubectl -n <ns> get svc rtsp-video-stream
```

Expected: pod is `1/1 Running`, service exposes port `8554`.

---

### 2) Start port-forward to RTSP server

**Terminal 1 (keep running):**

```bash
kubectl -n <ns> port-forward svc/rtsp-video-stream 8554:8554
```

Leave it running.

---

### 3) Start publishing the test RTSP stream

**Terminal 2 (keep running):** see the `docker run` command above. Leave it
running.

> If you see `Broken pipe` here, Terminal 1 likely stopped. Restart
> Terminal 1, then rerun this.

---

### 4) Apply the **Pipeline** (recipe)

**Terminal 3:**

```bash
kubectl -n <ns> apply -f demo/pipeline_rtsp.yaml
```

**What this does:**
Creates a Pipeline CR (`pipeline-rtsp`) declaring:

- `spec.mode: stream`
- ordered filters (`spec.filters[]`):
  - `video-in` (reads RTSP frames from the source via `$(RTSP_URL)`)
  - `face-blur` (processes frames)
  - `webvis` (renders output)
- `spec.services[]` to expose webvis port `8080`

The Pipeline intentionally has **no inline `spec.source.*`** — the input
is referenced from a separate PipelineSource CR (next step).

---

### 5) Apply the **PipelineSource** (input)

**Terminal 3:**

```bash
kubectl -n <ns> apply -f demo/pipelinesource_rtsp.yaml
```

**What this does:**
Creates a PipelineSource CR (`pipelinesource-rtsp`) declaring the RTSP
input the run will consume:

- `spec.rtsp.host: rtsp-video-stream`
- `spec.rtsp.port: 8554`
- `spec.rtsp.path: /stream`

PipelineSource lives in its own CR so multiple PipelineInstances can
share the same input wiring (or swap inputs without editing the Pipeline
recipe).

---

### 6) Apply the **PipelineInstance** (execution)

**Terminal 3:**

```bash
kubectl -n <ns> apply -f demo/pipelineinstance_rtsp.yaml
```

**What this does:**
Creates a PipelineInstance CR (`pipelineinstance-rtsp`) that binds the
Pipeline (`spec.pipelineRef.name: pipeline-rtsp`) to the PipelineSource
(`spec.sourceRef.name: pipelinesource-rtsp`). The controller reconciles
it by creating a single-replica Deployment running the filter chain.

---

### 7) Verify the controller created the stream Deployment + pod

**Terminal 3:**

```bash
kubectl -n <ns> get deploy pipelineinstance-rtsp-deploy
kubectl -n <ns> get pods -l pipelineinstance=pipelineinstance-rtsp
```

> The label key on the pod is **`pipelineinstance`** (not `pipelinerun`)
> — see `internal/controller/pipelineinstance_controller_streaming.go`.

**What this proves (docs):**
Stream mode runs as **one single-replica Deployment per
PipelineInstance**.

Expected: pod becomes **`3/3 Running`** (one container per filter in the
chain).

---

### 8) Verify RTSP ingest is live (frames flowing)

**Terminal 3:**

```bash
kubectl -n <ns> logs -l pipelineinstance=pipelineinstance-rtsp -c video-in --tail=20
```

Expected line:

```
video open: rtsp://rtsp-video-stream:8554/stream (30.0 fps)
```

**What this proves (docs):**
The PipelineSource referenced by the PipelineInstance is producing
frames.

---

### 9) View end-to-end output in WebVis

**Terminal 3 (run while viewing):**

```bash
kubectl -n <ns> port-forward svc/pipelineinstance-rtsp-webvis-0 8080:8080
```

Open browser:

```
http://localhost:8080
```

Expected: **moving color bars**.

**What this proves (docs):**

- Filters are chained in order (`spec.filters[]`).
- Stream Deployment is running continuously.
- `spec.services[]` correctly exposed the webvis port.
- Frames traveled end-to-end:
  RTSP → video-in → face-blur → webvis → browser.

---

## Common failure + fix (from your run)

If your pod is `CrashLoopBackOff` and `video-in` logs say:

```
video ended, exiting...
```

That means the RTSP stream stopped. Fix:

1. Ensure Terminal 1 is still running.
2. Ensure Terminal 2 is still running.
3. Restart the streaming pod:

```bash
kubectl -n <ns> delete pod -l pipelineinstance=pipelineinstance-rtsp
```

The controller will recreate it and it should reconnect.

---

## Cleanup

Stop terminals in this order:

1. WebVis port-forward (Terminal 3)
2. ffmpeg publisher (Terminal 2)
3. RTSP port-forward (Terminal 1)

Delete demo resources (in dependency order — instance first, then sources
and recipes):

```bash
kubectl -n <ns> delete pipelineinstance pipelineinstance-rtsp
kubectl -n <ns> delete pipelinesource pipelinesource-rtsp
kubectl -n <ns> delete pipeline pipeline-rtsp
kubectl -n <ns> delete -f hack/rtsp-stream/
```
