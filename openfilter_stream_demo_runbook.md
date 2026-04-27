# OpenFilter Pipelines Controller — Stream Mode E2E Demo

This runbook follows the **OpenFilter Pipelines Controller** docs. It shows a full end‑to‑end streaming demo using the repo manifests (`demo/pipeline_rtsp.yaml`, `demo/pipelinerun_rtsp.yaml`) and the demo RTSP server under `hack/rtsp-stream/`.

**Doc concepts you are demonstrating**

- **Pipeline (recipe):** declares the input source and ordered filter containers.  
- **PipelineRun (execution):** references a Pipeline and runs it.  
- **Stream mode:** real‑time RTSP processing via a **single‑replica Deployment** created by the controller. Optional Services expose filter ports.  

---

## Prereqs (from docs)

- Controller installed in your cluster (Helm install already done).
- `kubectl` working against the cluster.
- Repo cloned locally: `openfilter-pipelines-controller`.

---

## Terminals you'll use

You will use **3 terminals**. Two of them must keep running during the demo.

### Terminal 1 — KEEP RUNNING
RTSP port‑forward

```bash
kubectl -n openfilter-system port-forward svc/rtsp-video-stream 8554:8554
```

**What this does:**  
Creates a tunnel from your laptop → in‑cluster RTSP demo Service so your local RTSP publisher can push to it.

**Why the docs need it:**  
The stream demo requires an RTSP endpoint. This makes the demo RTSP server reachable from your machine.

---

### Terminal 2 — KEEP RUNNING
Publish a live test stream to `/stream`

```bash
docker run --rm -it --platform linux/amd64   jrottenberg/ffmpeg:6.1-alpine   -re -f lavfi -i testsrc=size=1280x720:rate=30   -f lavfi -i sine=frequency=1000   -rtsp_transport tcp   -c:v libx264 -tune zerolatency -pix_fmt yuv420p -preset veryfast   -c:a aac   -f rtsp   rtsp://host.docker.internal:8554/stream
```

**What this does:**  
Continuously publishes a color‑bar test video into the RTSP demo server at path `/stream`.

**Why the docs need it:**  
Stream mode needs `spec.source.rtsp` to be a real, live RTSP stream. This is your live input.

---

### Terminal 3 — Setup + validation
Use this for applying manifests and checking status/logs.

---

## Demo steps (E2E)

### 1) Deploy the demo RTSP server (in‑cluster)
**Terminal 3:**

```bash
kubectl -n openfilter-system apply -f hack/rtsp-stream/
```

**What this does:**  
Creates the demo RTSP Deployment + Service (`rtsp-video-stream`) inside the cluster.

**Validate:**

```bash
kubectl -n openfilter-system get pods -l app=rtsp-video-stream
kubectl -n openfilter-system get svc rtsp-video-stream
```

Expected: pod is `1/1 Running`, service exposes port `8554`.

---

### 2) Start port‑forward to RTSP server
**Terminal 1 (keep running):**

```bash
kubectl -n openfilter-system port-forward svc/rtsp-video-stream 8554:8554
```

Leave it running.

---

### 3) Start publishing the test RTSP stream
**Terminal 2 (keep running):**

```bash
docker run --rm -it --platform linux/amd64   jrottenberg/ffmpeg:6.1-alpine   -re -f lavfi -i testsrc=size=1280x720:rate=30   -f lavfi -i sine=frequency=1000   -rtsp_transport tcp   -c:v libx264 -tune zerolatency -pix_fmt yuv420p -preset veryfast   -c:a aac   -f rtsp   rtsp://host.docker.internal:8554/stream
```

Leave it running.

> If you see `Broken pipe` here, Terminal 1 likely stopped. Restart Terminal 1, then rerun this.

---

### 4) Apply the **Pipeline** (recipe)
**Terminal 3:**

```bash
kubectl -n openfilter-system apply -f demo/pipeline_rtsp.yaml
```

**What this does:**  
Creates a Pipeline CR declaring:
- `spec.mode: stream`
- RTSP source (`spec.source.rtsp`)
- ordered filters (`spec.filters[]`), typically:
  - `video-in` (reads RTSP)
  - `face-blur` (processes frames)
  - `webvis` (renders output)
- `spec.services[]` to expose webvis port 8080

---

### 5) Apply the **PipelineRun** (execution)
**Terminal 3:**

```bash
kubectl -n openfilter-system apply -f demo/pipelinerun_rtsp.yaml
```

**What this does:**  
Creates a PipelineRun CR that references the Pipeline.  
The controller reconciles it by creating a Deployment for the run.

---

### 6) Verify the controller created the stream Deployment + pod
**Terminal 3:**

```bash
kubectl -n openfilter-system get deploy pipelinerun-rtsp-deploy
kubectl -n openfilter-system get pods -l pipelinerun=pipelinerun-rtsp
```

**What this proves (docs):**  
Stream mode runs as **one single‑replica Deployment per PipelineRun**.

Expected: pod becomes **`3/3 Running`**.

---

### 7) Verify RTSP ingest is live (frames flowing)
**Terminal 3:**

```bash
kubectl -n openfilter-system logs -l pipelinerun=pipelinerun-rtsp -c video-in --tail=20
```

Expected line:

```
video open: rtsp://rtsp-video-stream:8554/stream (30.0 fps)
```

**What this proves (docs):**  
The required `spec.source.rtsp` input is valid and producing frames.

---

### 8) View end‑to‑end output in WebVis
**Terminal 3 (run while viewing):**

```bash
kubectl -n openfilter-system port-forward svc/pipelinerun-rtsp-webvis-0 8080:8080
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
- Frames traveled end‑to‑end:  
  RTSP → video‑in → face‑blur → webvis → browser.

---

## Common failure + fix (from your run)

If your pod CrashLoopBackOff and `video-in` logs say:

```
video ended, exiting...
```

That means the RTSP stream stopped. Fix:
1. Ensure Terminal 1 is still running.
2. Ensure Terminal 2 is still running.
3. Restart the streaming pod:

```bash
kubectl -n openfilter-system delete pod -l pipelinerun=pipelinerun-rtsp
```

The controller will recreate it and it should reconnect.

---

## Cleanup

Stop terminals in this order:
1. WebVis port‑forward (Terminal 3)
2. ffmpeg publisher (Terminal 2)
3. RTSP port‑forward (Terminal 1)

Delete demo resources:

```bash
kubectl -n openfilter-system delete pipelinerun pipelinerun-rtsp
kubectl -n openfilter-system delete pipeline pipeline-rtsp
kubectl -n openfilter-system delete -f hack/rtsp-stream/
```
