# OpenFilter Pipelines Runner

## 1. Summary

We're building a Kubernetes-native batch executor that processes large file sets through a configurable pipeline of containerized filters. Execution is triggered via an HTTP API (served by the controller binary). The API lists files in an S3-compatible bucket, enqueues one Redis Stream message per file, and creates a PipelineRun CR. The controller reconciles PipelineRun by creating exactly one Kubernetes Job whose pods each process one message: an init container claims a message + downloads the file, filter containers run (in parallel or chained), the pod terminates, and the controller ACKs the message and decides requeue/DLQ policies.

### Key Attributes

- **Kubernetes-native orchestration** via a single Job per run (simple lifecycle, easy ops)
- **At-least-once delivery** using Redis Streams consumer groups
- **Compact CRD status**; detailed outcomes via logs/streams/optional manifest
- **Works at 10k–100k+ files** with bounded API server load

## 2. Goals & Non-Goals

### Goals

- Trigger pipelines via an HTTP API deployed with the controller
- Pipeline defined as a CRD (Pipeline) with list of filters (images/args/env) and input bucket
- Each PipelineRun creates one Job with `completions = totalFiles`, `parallelism = user-tunable`
- Init container per pod: claim one message from Redis Stream and stage input from S3, then exit
- Filters as normal containers run concurrently (or sequentially) against staged input
- Controller (not pods) ACKs messages and handles retries/DLQ
- Scalable to thousands of files without per-file Job objects
- Compact, informative `PipelineRun.status`

### Non-Goals

- Complex DAG orchestration (Argo/Tekton territory)
- Exactly-once semantics (we use at-least-once + idempotent outputs)
- In-pod Redis ACKs (controller is the authority)

## 3. CRDs

### 3.1 Pipeline (recipe)

```yaml
apiVersion: pipelines.example.io/v1alpha1
kind: Pipeline
metadata:
  name: <pipeline-name>
spec:
  input:
    # bucket is the name of the S3-compatible bucket (required)
    bucket: my-input-bucket

    # prefix is an optional path prefix within the bucket (e.g., "input-data/")
    prefix: "images/"

    # endpoint is the S3-compatible endpoint URL (required for non-AWS S3)
    # Leave empty for AWS S3 (will use default AWS endpoints)
    endpoint: "http://minio.example.com:9000"

    # region is the bucket region (e.g., "us-east-1")
    # Required for AWS S3, optional for other providers
    region: "us-east-1"

    # credentialsSecret references a Secret containing access credentials
    # Expected keys: "accessKeyId" and "secretAccessKey"
    credentialsSecret:
      name: s3-credentials
      namespace: default  # optional, defaults to Pipeline namespace

    # insecureSkipTLSVerify skips TLS certificate verification (useful for dev/test)
    insecureSkipTLSVerify: false

    # usePathStyle forces path-style addressing (endpoint.com/bucket vs bucket.endpoint.com)
    # Required for MinIO and some S3-compatible services
    usePathStyle: true

  # filters is an ordered list of processing steps (executed sequentially)
  # At least one filter is required
  filters:
    - name: resize
      image: ghcr.io/acme/resize:1.2.3
      command: ["/bin/resize"]  # optional, overrides default entrypoint
      args: ["--width=1024", "--height=768"]
      env:
        - name: LOG_LEVEL
          value: "info"
        - name: OUTPUT_FORMAT
          value: "jpeg"
      resources:
        requests:
          memory: "512Mi"
          cpu: "500m"
        limits:
          memory: "1Gi"
          cpu: "1000m"
      imagePullPolicy: IfNotPresent  # Always, Never, or IfNotPresent

    - name: denoise
      image: ghcr.io/acme/denoise:2.0.0
      args: ["--strength=0.2"]
      env:
        - name: ALGORITHM
          value: "gaussian"
      resources:
        requests:
          memory: "256Mi"
          cpu: "250m"
```

### 3.2 PipelineRun (execution)

```yaml
apiVersion: pipelines.example.io/v1alpha1
kind: PipelineRun
metadata:
  name: <pipeline-name>-<ts>
spec:
  pipelineRef: <pipeline-name>
  execution:
    parallelism: 200            # max concurrent pods
    maxAttempts: 3              # per-file
    pendingTimeout: 900000        # Redis re-claim time (15m)
  queue:
    stream: pr:<runId>:work
    group: cg:<runId>
    redisSecretRef: redis-creds
status:
  counts:
    totalFiles: 0
    queued: 0
    running: 0
    succeeded: 0
    failed: 0
  conditions: []
  jobName: ""
  startTime: null
  completionTime: null
```

## 4. High-Level Architecture

### API (inside controller) — `POST /pipelines/{name}/runs`

1. Loads Pipeline
2. Lists S3 (ListObjectsV2 with prefix/suffix), streams files
3. Ensures `XGROUP CREATE` for `pr:<runId>`
4. `XADD` one message per file: `{run, file, attempts=0}`
5. Creates PipelineRun with totalFiles, stream, group, and execution settings

### Reconciler (PipelineRun)

1. Creates one Job (owner-referenced) with `completions=totalFiles`, `parallelism=...`
2. Watches Pods; on Succeeded/Failed, performs ACK/retry/DLQ via Redis
3. Periodically updates `status.counts` using Redis metrics (XINFO, XPENDING, XLEN)
4. Runs a reclaimer (XAUTOCLAIM) for stale pending entries
5. Marks run Succeeded when `(queued==0 && pending==0)` and Job is Complete

## 5. Job & Pod Design

### 5.1 Job spec (generated by controller)

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pr-{{ runId }}
  labels:
    pipelines.example.io/run: "{{ runId }}"
spec:
  completionMode: NonIndexed
  completions: {{ .status.counts.totalFiles }}
  parallelism: {{ .spec.execution.parallelism }}
  backoffLimit: 2
  ttlSecondsAfterFinished: 86400
  template:
    metadata:
      labels:
        pipelines.example.io/run: "{{ runId }}"
    spec:
      serviceAccountName: pipeline-exec
      restartPolicy: Never
      volumes:
        - name: ws
          emptyDir: {}
        - name: ctrl
          emptyDir: {}
      initContainers:
        - name: claimer
          image: ghcr.io/yourorg/worker-utils:1.0.0
          command: ["/claimer"]
          env:
            - { name: STREAM, value: "{{ .spec.queue.stream }}" }
            - { name: GROUP,  value: "{{ .spec.queue.group }}" }
            - { name: REDIS_URL, valueFrom: { secretKeyRef: { name: {{ .spec.queue.redisSecretRef }}, key: url } } }
            - { name: VISIBILITY_MS, value: "{{ .spec.execution.pendingTimeout }}" }
          volumeMounts:
            - { name: ws,   mountPath: /ws }
            - { name: ctrl, mountPath: /ctrl }
      containers:
        # Example: two filters; add more per Pipeline
        - name: resize
          image: ghcr.io/yourorg/resize:1.2.3
          command: ["/bin/sh","-lc"]
          args: ["set -e; test -f /ws/in/input; resize --in /ws/in --out /ws/out/resize --width 1024 --height 768"]
          volumeMounts:
            - { name: ws,   mountPath: /ws }
            - { name: ctrl, mountPath: /ctrl }
        - name: denoise
          image: ghcr.io/yourorg/denoise:2.0.0
          command: ["/bin/sh","-lc"]
          args: ["set -e; denoise --in /ws/out/resize --out /ws/out/denoise --strength 0.2"]
          volumeMounts:
            - { name: ws,   mountPath: /ws }
            - { name: ctrl, mountPath: /ctrl }
```

### 5.2 Init "claimer" behavior

1. `XREADGROUP GROUP cg:<runId> <pod> COUNT 1 BLOCK 0 STREAMS pr:<runId>:work ">"` → `{mid, file, attempts}` (no ACK)
2. Download file → `/ws/in/input` (S3 client with IRSA/Workload Identity creds)
3. Patch own Pod annotations with:
   - `queue.redis.mid`
   - `queue.file`
   - `queue.attempts`
4. Exit → filter containers start
5. **RBAC (worker SA)**: needs `get`, `patch` on pods in its namespace

### 5.3 Filter containers

- Run concurrently by default (or encode ordering via sentinel files or dependency checks)
- On error, exit non-zero → Pod becomes Failed
- On success, all exit 0 → Pod Succeeded

## 6. Controller Responsibilities

### 6.1 API handler

**Input:** `POST /pipelines/{name}/runs` with optional body overrides `{prefix,suffix,parallelism,maxAttempts,pendingTimeout}` and `Idempotency-Key` header.

**Steps:**

1. Get Pipeline
2. List S3 with filters; stream keys
3. Create Redis stream + group if absent
4. `XADD` one message per file: `{run, file, attempts=0}`
5. Create PipelineRun with execution + queue info; set `status.counts.totalFiles`
6. Return `201 {runId, pipelineRun, totalFiles}`

### 6.2 Reconciler loop

1. **Ensure Job exists** and matches spec; set OwnerReference
2. **Pod watch:** on pod completion, read annotations and perform:
   - **Succeeded:** `XACK(stream, group, mid)`; optionally `XADD` results
   - **Failed:** if `attempts+1 < maxAttempts` → `XADD` back with incremented attempts; else `XADD` to DLQ; in both cases `XACK` original
3. **Reclaimer:** run `XAUTOCLAIM(stream, group, "controller", pendingTimeout, COUNT=100)` to recover messages claimed by pods that died pre-completion
4. **Status:** periodically compute:
   - `queued := XINFO STREAM stream len`
   - `running := XPENDING stream group count`
   - `failed := XLEN dlq`
   - `succeeded := totalFiles - queued - failed - running` (or via results stream)
5. **Completion:** when `(queued==0 && running==0)` and Job is Complete → mark PipelineRun Succeeded

## 7. Data Model & Naming

### Redis keys per run

- **Work stream:** `pr:<runId>:work`
- **Consumer group:** `cg:<runId>`
- **DLQ:** `pr:<runId>:dlq`
- **Results (optional):** `pr:<runId>:results`

### Message fields

- `run`, `file`, `attempts`

### Pod annotations

- `queue.redis.mid`
- `queue.file`
- `queue.attempts`

### Labels

- `pipelines.example.io/run=<runId>` on Job/Pods

## 8. Failure Modes & Recovery

### Quick Reference

- **Init claimed then pod/node died:** message stays pending; reclaimer XAUTOCLAIM rehomes; controller re-enqueues (attempts++) or DLQs; XACK reclaimed
- **Filter crashes:** Pod Failed; controller retries per maxAttempts then DLQs; Job controller creates replacement pod until completions
- **S3 transient errors:** init download fails → pod Failed → retry path
- **Redis outage:** init blocks on XREADGROUP; reconciler pauses; Job pods back off
- **Oversized buckets:** API streams XADD without buffering; Redis memory sizing required

### How failures are handled (end-to-end)

#### 1. Where a failure can occur

##### A. Init container (claimer/stager)

- **Before claim** → no message involved; pod fails; Job spawns a new pod
- **After claim, before annotations/download completes** → message is pending in Redis (PEL) under that pod's consumer; the controller's reclaimer (XAUTOCLAIM after pendingTimeout) takes it back and re-queues (or DLQs), then a new pod will claim it
- **After claim, after annotations patch, download fails** → pod Failed; controller sees mid/file/attempts on the pod annotations → re-enqueue (attempts+1) or DLQ, then ACK the original mid

##### B. Filter containers (concurrent steps)

- Any filter exits non-zero → pod phase = Failed
- Controller reads mid/file/attempts (from annotations written by the init), and:
  - if `attempts+1 < maxAttempts` → re-enqueue new message with incremented attempts, ACK old mid
  - else → DLQ (with a reason), ACK old mid
- Job will create replacement pods until it reaches the configured completions

##### C. Node/preemption/OOM/image-pull/etc.

- Pod dies unexpectedly → init may or may not have claimed a message:
  - **If it didn't:** nothing to do; Job retries
  - **If it did:** message is pending in Redis → reclaimer recovers it after pendingTimeout → re-enqueue or DLQ, then ACK reclaimed entry

#### 2. What the Job controller does vs what our controller does

- **Kubernetes Job controller:** replaces failed pods until the Job reaches completions successes or hits backoffLimit. It doesn't know about Redis.
- **Our controller:** is the source of truth for the queue. It watches pod completion events and performs:
  - Succeeded → `XACK(mid)`
  - Failed → re-enqueue/DLQ (based on attempts), then `XACK(mid)`
  - Periodic reclaimer to recover orphaned pending messages (XAUTOCLAIM)

This separation means the Job keeps the right number of workers running, while our controller guarantees queue correctness.

#### 3. Concrete failure scenarios

- **Filter crash (transient):** Pod Failed → controller re-enqueues with attempts+1 → Job spawns a new pod → init claims a new message for that file → try again
- **Bad input (permanent):** Filters consistently error with e.g. "invalid format" → attempts reaches maxAttempts → controller pushes to dlq with reason → ACKs original → Job continues with other files
- **Init claimed, node died:** Message stuck pending → controller reclaimer XAUTOCLAIM after pendingTimeout → re-enqueue and ACK reclaimed → new pod processes it
- **Image pull error:** Pod never starts filters → Pod Failed → controller re-enqueues and ACKs; Job retries depending on backoffLimit. (Operationally you should alert on this class of failure.)
- **OOMKilled:** Treat as filter failure; same retry/DLQ path. Also add resource limits/requests per filter image to avoid flapping.

### What shows up in status

`PipelineRun.status.counts` is kept compact and derives from Redis:

- `queued = XINFO STREAM <work>.length`
- `running = XPENDING <work> <group>.count`
- `failed = XLEN <dlq>`
- `succeeded = totalFiles - queued - running - failed` (or count from a results stream if you keep one)
- `recentFailures[]` (bounded list) include file, attempts, and reason (e.g., exit code, OOM)

Conditions move Running → Succeeded only when `queued == 0` and `running == 0` and the Job reports Complete.

## 9. Security & Permissions

### S3

- IRSA/Workload Identity bound to `serviceAccountName: pipeline-exec`

### Kubernetes RBAC

- **Worker SA:** `get`, `patch` on Pods in namespace (to annotate mid/file/attempts)
- **Controller:** CRUD on PipelineRun, Jobs, Pods (read), status updates

### Redis ACLs

- **Worker init:** XREADGROUP only (no XACK)
- **Controller:** XGROUP, XADD, XACK, XPENDING, XINFO, XAUTOCLAIM

### NetworkPolicy

- Allow controller + workers → Redis; restrict others

## 10. Observability

### Metrics (controller)

- Queue depth, pending count, DLQ length
- Throughput (files/min)
- Retries, success ratio, run duration

### Logs

- Per pod (filters)
- Controller ACK/retry decisions, reclaimer actions

### Tracing

- Optional span per file from claim → ACK

### Status surfaces

- Compact counters + recentFailures sample (bounded list)

## 11. Scaling & Performance

- **Concurrency:** tune parallelism per run; Job controller throttles pod creation; cluster autoscaler provisions nodes
- **Throughput:** bounded by filter CPU/IO and S3/Redis limits; consider s5cmd/minio mc for faster downloads
- **Retry policy:** exponential backoff per re-enqueue (store attempts, optionally next_at)
- **Large runs:** memory-size Redis to hold N messages; trim results stream; GC after completion

## 12. Testing Strategy

### Unit

- API handler (S3 paging, XADD calls)
- Claimer (parsing, annotation patch)
- Reconciler (ACK/retry)
- Reclaimer

### Integration (kind/minio/redis)

- **Small run:** 100 files, parallelism=20, inject 5% failures → verify retries/DLQ
- **Crash resilience:** kill nodes mid-run → ensure XAUTOCLAIM recovery
- **Scale:** 50k messages, observe stable API server usage and Job completion

### E2E

- Happy path, DLQ path, idempotent reruns (same runId not reused)

## 13. Rollout Plan

1. Deploy Redis (HA) and MinIO (if not using cloud S3); create redis-creds secret
2. Deploy controller + CRDs
3. Dry-run with test pipeline (tiny filters) and small bucket
4. Gradually raise parallelism while watching cluster utilization and Redis metrics
5. Enable autoscaling (nodes) and adjust resource limits per filter
6. Add alerts on DLQ growth, high retries, long pending entries

## 14. Open Questions

- Should we persist a run manifest (JSONL) to the bucket summarizing outcomes for audit?
- Do we need output configuration (e.g., destination bucket for processed files)?
- Should we support file filtering by suffix/pattern in the ObjectStorageSource spec?
