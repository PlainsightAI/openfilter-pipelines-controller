# OpenFilter Pipelines Controller

## 1. Summary

We're building a Kubernetes-native batch executor that processes large file sets through a configurable pipeline of containerized filters. Users create a Pipeline CRD (defining input bucket and filters) and a PipelineRun CRD (referencing the pipeline with execution parameters). The PipelineRun controller reconciles by: (1) listing files from S3-compatible storage, (2) enqueuing one Valkey Stream message per file, (3) creating exactly one Kubernetes Job whose pods each process one message via an init container that claims + downloads, then filter containers run, and (4) watching pod completions to ACK messages and handle retries/DLQ.

### Key Attributes

- **Fully declarative**: No HTTP API - everything via Kubernetes CRDs
- **Kubernetes-native orchestration** via a single Job per run (simple lifecycle, easy ops)
- **At-least-once delivery** using Valkey Streams consumer groups
- **Compact CRD status**; detailed outcomes via logs/streams
- **Designed for 10k–100k+ files** with bounded API server load

## 2. Goals & Non-Goals

### Goals

- Fully declarative Kubernetes-native workflow via CRDs
- Pipeline defined as a CRD with input source (S3-compatible bucket) and ordered filters
- PipelineRun CRD references a Pipeline and provides execution parameters
- PipelineRun controller handles:
  - S3 file listing and Valkey stream initialization on first reconciliation
  - Job creation with `completions = totalFiles`, `parallelism = user-tunable`
  - Pod completion tracking for ACK/retry/DLQ decisions
  - Status updates from Valkey metrics
- Init container per pod: claim one message from Valkey Stream (consumer name = pod name), download from S3 to /ws, then exit (no pod annotations)
- Filters as normal containers run sequentially against staged input
- Controller (not pods) ACKs messages and handles retries/DLQ
- Scalable to thousands of files without per-file Job objects
- Compact, informative `PipelineRun.status`

### Non-Goals

- HTTP API for triggering runs (use kubectl apply instead)
- Complex DAG orchestration (Argo/Tekton territory)
- Exactly-once semantics (we use at-least-once + idempotent outputs)
- In-pod Valkey ACKs (controller is the authority)

## 3. CRDs

### 3.1 Pipeline (recipe)

```yaml
apiVersion: filter.plainsight.ai/v1alpha1
kind: Pipeline
metadata:
  name: <pipeline-name>
spec:
  # source defines where to read input files from
  source:
    bucket:
      # name is the S3-compatible bucket name (required)
      name: my-input-bucket

      # prefix is an optional path prefix within the bucket (e.g., "input-data/")
      prefix: "images/"

      # endpoint is the S3-compatible endpoint URL (required for non-AWS S3)
      # Leave empty for AWS S3 (will use default AWS endpoints)
      # Examples: "storage.googleapis.com", "http://minio.example.com:9000"
      endpoint: "storage.googleapis.com"

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
      usePathStyle: false

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
apiVersion: filter.plainsight.ai/v1alpha1
kind: PipelineRun
metadata:
  name: <pipeline-name>-<timestamp>
spec:
  # pipelineRef references the Pipeline to execute
  pipelineRef:
    name: <pipeline-name>
    namespace: default  # optional, defaults to PipelineRun namespace

  # execution defines how the pipeline should be executed
  execution:
    parallelism: 10         # max concurrent pods (default: 10)
    maxAttempts: 3          # per-file retry limit (default: 3)
    pendingTimeout: "15m"   # Valkey re-claim time (default: 15m)

  # queue defines the Valkey stream configuration
  # The controller uses these keys to manage work distribution
  queue:
    stream: "pr:<runId>:work"  # format: pr:<runId>:work
    group: "cg:<runId>"         # format: cg:<runId>

# Status is managed by the controller
status:
  counts:
    totalFiles: 0    # set during initialization
    queued: 0        # computed from Valkey consumer group lag
    running: 0       # computed from Valkey pending count
    succeeded: 0     # computed: totalFiles - queued - running - failed
    failed: 0        # computed from DLQ length
  conditions: []     # Progressing, Succeeded, Degraded
  jobName: ""        # name of the created Job
  startTime: null
  completionTime: null
```

## 4. High-Level Architecture

### User Workflow

1. Create a Pipeline CRD defining input source and filters
2. Create a PipelineRun CRD referencing the Pipeline (with unique queue keys)
3. Watch PipelineRun status for progress (queued/running/succeeded/failed counts)
4. Inspect Valkey DLQ stream for failures: `pr:<runId>:dlq`

### PipelineRun Controller Reconciliation Loop

The controller implements the following steps on each reconciliation:

**Phase 1: Initialization (first reconciliation only)**
1. Validates Pipeline reference and fetches Pipeline spec
2. Creates Valkey stream (`pr:<runId>:work`) and consumer group (`cg:<runId>`)
3. Lists S3 bucket files using `ListObjects` (streaming, non-buffered)
4. Enqueues one `XADD` message per file: `{run, file, attempts=0}`
5. Sets `status.counts.totalFiles` and `status.startTime`

**Phase 2: Job Management**
1. Creates one Job (owner-referenced) with:
   - `completions = totalFiles`
   - `parallelism = spec.execution.parallelism`
   - Init container: claimer (claims message, downloads file)
   - Main containers: filter containers from Pipeline spec

**Phase 3: Pod Completion Handling**
1. Lists pods with label `filter.plainsight.ai/run=<runId>`
2. For each completed pod (Succeeded/Failed):
   - Uses XPENDING with `consumer=<pod.name>` to find the pending entry, then XRANGE(mid,mid) to read `{file, attempts}`
   - **Succeeded**: ACKs message via `XACK`
   - **Failed**:
     - If `attempts+1 < maxAttempts`: re-enqueues with `XADD` (incremented attempts), then ACKs original
     - Else: adds to DLQ (`pr:<runId>:dlq`) with reason, then ACKs original

**Phase 4: Reclaimer (stale message recovery)**
1. Runs `XAUTOCLAIM(stream, group, "controller-reclaimer", pendingTimeout, 100)`
2. For each reclaimed message:
   - If `attempts+1 < maxAttempts`: re-enqueues, ACKs reclaimed entry
   - Else: adds to DLQ, ACKs reclaimed entry

**Phase 5: Status Update**
1. Queries Valkey metrics:
   - `queued = GetConsumerGroupLag(stream, group)` (unread messages)
   - `running = GetPendingCount(stream, group)` (claimed but not ACKed)
   - `failed = GetStreamLength(dlq)` (DLQ entries)
   - `succeeded = totalFiles - queued - running - failed`
2. Updates `PipelineRun.status.counts`

**Phase 6: Completion Detection**
1. If `queued == 0 && running == 0` and Job has `Complete` condition:
   - Sets `Succeeded` condition to True
   - Sets `completionTime`
   - Stops reconciliation
2. If Job has `Failed` condition:
   - Flushes remaining work to DLQ
   - Sets `Degraded` condition to True
   - Stops reconciliation

**Reconciliation Frequency**
- Requeues every 30s for status updates while running
- No requeue after completion/failure

## 5. Job & Pod Design

### 5.1 Job spec (generated by controller)

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ pipelineRunName }}-job
  labels:
    filter.plainsight.ai/run: "{{ runId }}"
    filter.plainsight.ai/pipelinerun: "{{ pipelineRunName }}"
spec:
  completionMode: NonIndexed
  completions: {{ status.counts.totalFiles }}
  parallelism: {{ spec.execution.parallelism }}
  backoffLimit: 2
  ttlSecondsAfterFinished: 86400  # 24 hours
  template:
    metadata:
      labels:
        filter.plainsight.ai/run: "{{ runId }}"
        filter.plainsight.ai/pipelinerun: "{{ pipelineRunName }}"
    spec:
      restartPolicy: Never
      volumes:
        - name: workspace
          emptyDir: {}
      initContainers:
        - name: claimer
          image: {{ claimerImage }}  # configured in controller
          env:
            - name: STREAM
              value: "{{ spec.queue.stream }}"
            - name: GROUP
              value: "{{ spec.queue.group }}"
            - name: VALKEY_URL
              value: "{{ valkeyAddr }}"  # from controller config
            - name: VALKEY_PASSWORD
              value: "{{ valkeyPassword }}"  # from controller config (if set)
            - name: CONSUMER_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: S3_BUCKET
              value: "{{ pipeline.spec.source.bucket.name }}"
            - name: S3_ENDPOINT
              value: "{{ pipeline.spec.source.bucket.endpoint }}"
            - name: S3_REGION
              value: "{{ pipeline.spec.source.bucket.region }}"
            - name: S3_USE_PATH_STYLE
              value: "{{ pipeline.spec.source.bucket.usePathStyle }}"
            - name: S3_INSECURE_SKIP_TLS_VERIFY
              value: "{{ pipeline.spec.source.bucket.insecureSkipTLSVerify }}"
            # S3 credentials from secret (if configured)
            - name: S3_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: "{{ pipeline.spec.source.bucket.credentialsSecret.name }}"
                  key: accessKeyId
            - name: S3_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: "{{ pipeline.spec.source.bucket.credentialsSecret.name }}"
                  key: secretAccessKey
          volumeMounts:
            - name: workspace
              mountPath: /ws
      containers:
        # Dynamically generated from Pipeline.spec.filters
        # Example: two filters
        - name: resize
          image: ghcr.io/acme/resize:1.2.3
          command: ["/bin/resize"]
          args: ["--width=1024", "--height=768"]
          env:
            - name: LOG_LEVEL
              value: "info"
          volumeMounts:
            - name: workspace
              mountPath: /ws
        - name: denoise
          image: ghcr.io/acme/denoise:2.0.0
          args: ["--strength=0.2"]
          volumeMounts:
            - name: workspace
              mountPath: /ws
```

### 5.2 Init "claimer" behavior

The claimer is a standalone Go binary (`cmd/claimer/main.go`) that runs as an init container:

1. **Connect to Valkey** with exponential backoff (handles transient Valkey outages)
2. **Claim message**: `XREADGROUP GROUP <group> <consumerName> COUNT 1 BLOCK 0 STREAMS <stream> ">"`
   - Returns: message ID and fields `{run, file, attempts}`
   - Consumer name is the pod name (enables XAUTOCLAIM recovery by controller)
   - Does NOT ACK (controller is responsible for ACK after pod completion)
3. **Download file from S3**:
   - Creates MinIO client using Pipeline bucket configuration
   - Downloads `file` to `/ws/input`
   - Supports AWS S3, GCS, MinIO, and other S3-compatible storage
4. **Exit successfully** → filter containers start

**Key behaviors**:
- Retries Valkey connection failures with exponential backoff (1s to 30s)
- Fails fast on auth errors, missing config, or S3 download failures
- If claimer fails, pod enters Failed state → controller handles retry/DLQ logic

**RBAC requirements**:
- Worker pods: no Kubernetes API access required. Access to S3 credential secrets is provided via env/secretRef.

### 5.3 Filter containers

- Run **sequentially** (standard Kubernetes container ordering)
- Each filter reads from `/ws/input` (or previous filter output in `/ws/out/<filter-name>`)
- Filters can write intermediate outputs to `/ws/out/<filter-name>/`
- All containers share the same `workspace` emptyDir volume
- On error: any filter exits non-zero → Pod becomes Failed
- On success: all filters exit 0 → Pod Succeeded
- Filters should be idempotent (at-least-once delivery semantics)

## 6. Controller Responsibilities

The PipelineRun controller (`internal/controller/pipelinerun_controller.go`) is the orchestrator for all pipeline execution logic.

### 6.1 Initialization (`initializePipelineRun`)

**Trigger**: First reconciliation (when `status.startTime == nil`)

**Steps**:
1. Fetch referenced Pipeline resource
2. Create Valkey stream and consumer group (idempotent via `XGROUP CREATE MKSTREAM`)
3. Check stream length; if 0, proceed with file enumeration:
   - Get S3 credentials from Pipeline's credentialsSecret
   - List bucket files using `minio.ListObjects` (streaming, non-buffered)
   - For each file: `XADD <stream> * run <runId> file <filepath> attempts 0`
4. Set `status.counts.totalFiles` from final stream length
5. Set `status.startTime`
6. Update status with `Progressing` condition

**Edge cases handled**:
- Empty bucket (0 files) → marks PipelineRun as Degraded, skips Job creation
- S3 credential errors → sets Degraded condition, returns error for retry
- Valkey unavailable → sets Degraded condition, returns error for retry
- Idempotent: safe to re-run if initialization partially failed

### 6.2 Job Management (`ensureJob`)

**Trigger**: Every reconciliation after initialization

**Steps**:
1. Check if Job already exists (via `status.jobName`)
2. If not, build Job spec:
   - Set `completions = status.counts.totalFiles`
   - Set `parallelism = spec.execution.parallelism`
   - Configure claimer init container with Valkey + S3 config
   - Configure filter containers from Pipeline.spec.filters
3. Set controller owner reference (enables cascading delete)
4. Create Job via Kubernetes API
5. Update `status.jobName`

### 6.3 Pod Completion Handler (`handleCompletedPods`)

**Trigger**: Every reconciliation (every 30s)

**Steps**:
1. List pods with label `filter.plainsight.ai/run=<runId>`
2. For each pod in Succeeded/Failed phase:
   - Use XPENDING with `consumer=<pod.name>` to find the pending entry and XRANGE to fetch fields `{file, attempts}`
   - **If Succeeded**:
     - `XACK <stream> <group> <mid>`
   - **If Failed**:
     - Parse failure reason (container exit code, OOMKilled, ImagePullBackOff, etc.)
     - If `attempts+1 < maxAttempts`:
       - `XADD <stream> * run <runId> file <file> attempts <attempts+1>`
       - `XACK <stream> <group> <mid>`
     - Else:
       - `XADD <dlq> * run <runId> file <file> attempts <attempts> reason <reason>`
       - `XACK <stream> <group> <mid>`
   - Mark pod processed: patch annotation `filter.plainsight.ai/processed=true`

**Special handling**:
- **Start failures** (ImagePullBackOff, CreateContainerError): detected early, trigger immediate retry/DLQ + pod deletion (allows Job controller to spawn replacement)

### 6.4 Reclaimer (`runReclaimer`)

**Trigger**: Every reconciliation (every 30s)

**Purpose**: Recover messages from pods that died before completing (node failures, preemptions, OOM, etc.)

**Steps**:
1. `XAUTOCLAIM <stream> <group> "controller-reclaimer" <pendingTimeout> 0 COUNT 100`
2. For each reclaimed message:
   - Parse `file` and `attempts` from message fields
   - If `attempts+1 < maxAttempts`:
     - Re-enqueue: `XADD <stream> * run <runId> file <file> attempts <attempts+1>`
   - Else:
     - DLQ: `XADD <dlq> * run <runId> file <file> attempts <attempts> reason "Max attempts exceeded (reclaimed stale message)"`
   - `XACK <stream> <group> <mid>`

### 6.5 Status Updater (`updateStatus`)

**Trigger**: Every reconciliation (every 30s)

**Steps**:
1. Query Valkey for current counts:
   - `queued = GetConsumerGroupLag(stream, group)` — unread messages in stream
   - `running = GetPendingCount(stream, group)` — messages claimed but not ACKed (PEL)
   - `failed = GetStreamLength(dlq)` — DLQ entries
   - `succeeded = totalFiles - queued - running - failed`
2. Update `status.counts` with computed values

**Implementation notes**:
- Uses Valkey `XINFO GROUPS` to get consumer group lag
- Uses Valkey `XPENDING <stream> <group>` summary for pending count
- Uses Valkey `XLEN <dlq>` for failure count

### 6.6 Completion Detection (`checkCompletion`, `checkFailure`)

**Completion criteria** (`checkCompletion`):
- `status.counts.queued == 0`
- `status.counts.running == 0`
- Job has `Complete` condition with status True

**Failure criteria** (`checkFailure`):
- Job has `Failed` condition with status True
- Triggers `flushOutstandingWork` to move remaining queue entries to DLQ
- Sets `Degraded` condition on PipelineRun

**Actions on completion/failure**:
- Set `status.completionTime`
- Update conditions
- Stop requeuing (no further reconciliation needed)

## 7. Data Model & Naming

### Valkey keys per run

- **Work stream:** `pr:<runId>:work`
- **Consumer group:** `cg:<runId>`
- **DLQ:** `pr:<runId>:dlq`
- **Results (optional):** `pr:<runId>:results`

### Message fields

- `run`, `file`, `attempts`

### Pod annotations

Not used; message identity is derived from the Valkey consumer mapping (consumer = pod name).

### Labels

- `pipelines.example.io/run=<runId>` on Job/Pods

## 8. Failure Modes & Recovery

### Quick Reference

- **Init claimed then pod/node died:** message stays pending; reclaimer XAUTOCLAIM rehomes; controller re-enqueues (attempts++) or DLQs; XACK reclaimed
- **Filter crashes:** Pod Failed; controller retries per maxAttempts then DLQs; Job controller creates replacement pod until completions
- **S3 transient errors:** init download fails → pod Failed → retry path
- **Valkey outage:** init blocks on XREADGROUP; reconciler pauses; Job pods back off
- **Oversized buckets:** API streams XADD without buffering; Valkey memory sizing required

### How failures are handled (end-to-end)

#### 1. Where a failure can occur

##### A. Init container (claimer/stager)

- **Before claim** → no message involved; pod fails; Job spawns a new pod
- **After claim, before download completes** → message is pending in Valkey (PEL) under that pod's consumer; the controller's reclaimer (XAUTOCLAIM after pendingTimeout) takes it back and re-queues (or DLQs), then a new pod will claim it
- **After claim, download fails** → pod Failed; controller reads `{file, attempts}` via XRANGE(mid,mid) → re-enqueue (attempts+1) or DLQ, then ACK the original mid

##### B. Filter containers (concurrent steps)

- Any filter exits non-zero → pod phase = Failed
- Controller fetches `{file, attempts}` for the pod’s pending entry and:
  - if `attempts+1 < maxAttempts` → re-enqueue new message with incremented attempts, ACK old mid
  - else → DLQ (with a reason), ACK old mid
- Job will create replacement pods until it reaches the configured completions

##### C. Node/preemption/OOM/image-pull/etc.

- Pod dies unexpectedly → init may or may not have claimed a message:
  - **If it didn't:** nothing to do; Job retries
  - **If it did:** message is pending in Valkey → reclaimer recovers it after pendingTimeout → re-enqueue or DLQ, then ACK reclaimed entry

#### 2. What the Job controller does vs what our controller does

- **Kubernetes Job controller:** replaces failed pods until the Job reaches completions successes or hits backoffLimit. It doesn't know about Valkey.
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

`PipelineRun.status.counts` is kept compact and derives from Valkey:

- `queued = XINFO STREAM <work>.length`
- `running = XPENDING <work> <group>.count`
- `failed = XLEN <dlq>`
- `succeeded = totalFiles - queued - running - failed` (or count from a results stream if you keep one)
- `recentFailures[]` (bounded list) include file, attempts, and reason (e.g., exit code, OOM)

Conditions move Running → Succeeded only when `queued == 0` and `running == 0` and the Job reports Complete.

## 9. Security & Permissions

### S3

- IRSA/Workload Identity (optional) for object storage if needed; no special worker ServiceAccount required

### Kubernetes RBAC

- **Worker pods:** no Kubernetes API access required
- **Controller:** CRUD on PipelineRun, Jobs; read/watch Pods; status updates

### Valkey ACLs

- **Worker init:** XREADGROUP only (no XACK)
- **Controller:** XGROUP, XADD, XACK, XPENDING, XINFO, XAUTOCLAIM

### NetworkPolicy

- Allow controller + workers → Valkey; restrict others

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

### Current Design Scalability for 10k Files

**Architecture strengths**:
- Single Job per run (not 10k Jobs) → minimal API server load
- Valkey Streams handle 10k+ messages efficiently
- Compact status (aggregate counts only, not per-file state)
- Streaming S3 list (non-buffered iteration via `ListObjects` channel)

**Identified bottlenecks** for 10k+ files:

#### 1. **S3 List + XADD Loop** (Initialization Phase)
**Current behavior**: Controller lists all files from S3 and performs individual `XADD` calls sequentially.

**Impact at 10k files**:
- S3 ListObjects: ~1-3 seconds (paginated, fast)
- 10k serial XADD calls: ~10-30 seconds total
- Reconciliation blocks during this time

**Mitigations**:
- Already idempotent (checks stream length before listing)
- **Recommended**: Pipeline XADD calls (batch 100-500 commands)
- **Consider**: Report progress via status field (`status.counts.enqueued`)

#### 2. **Pod Annotation Patching** (Limited by Parallelism)
**Current behavior**: Each pod's claimer init container patches its own annotations via Kubernetes API.

**Impact at 10k files with parallelism=10**:
- **Maximum 10 concurrent PATCH requests** at any time (bounded by parallelism)
- As pods complete, new pods start and patch → ~10 PATCH/30s steady state
- **Total 10k PATCH requests over the run lifetime**, not concurrent
- API server load is **bounded and predictable**

**Impact at higher parallelism** (e.g., parallelism=200):
- ~200 PATCH requests during initial pod startup burst
- Then ~200/minute as pods cycle through
- Still manageable for most API servers

**Current mitigations in claimer**:
- 10s timeout on patch request
- Pod annotation patching is NOT a bottleneck due to parallelism limiting

**Recommended improvements**:
- Add retry with backoff + jitter for robustness (low priority)
- No urgent action needed - design naturally limits load

#### 3. **Pod Listing on Every Reconciliation** (Every 30s)
**Current behavior**: Controller lists all pods with run label every 30s to check for completions.

**Impact at 10k files with parallelism=10**:
- **Maximum ~10-20 active pods** at any time (parallelism + completed/failed waiting for cleanup)
- List call returns only active pods, not all 10k
- Each pod object ~2-5KB → **50-100KB of data per list** (very manageable)
- Informer cache memory: **<1MB** per PipelineRun
- **This is NOT a bottleneck**

**Impact at higher parallelism** (e.g., parallelism=200):
- ~200-400 active pods (active + recently completed)
- List returns ~1-2MB of data per reconciliation
- Still very manageable

**Why completed pods don't accumulate**:
- Pods with `restartPolicy: Never` stay around after completion
- Job controller's `ttlSecondsAfterFinished: 86400` eventually cleans them up
- Controller marks pods `processed=true` and skips them in subsequent reconciliations

**Mitigations**:
- Label selector reduces scope (`filter.plainsight.ai/run=<runId>`)
- Skip already-processed pods (`filter.plainsight.ai/processed=true` annotation)
- Parallelism naturally bounds active pod count
- **Optional**: Aggressive TTL (e.g., 3600s) to clean up completed pods faster

#### 4. **Reclaimer XAUTOCLAIM** (COUNT=100 per call)
**Current behavior**: Controller runs `XAUTOCLAIM ... COUNT 100` every 30s.

**Impact at 10k files with parallelism=10**:
- **Normal operation**: Only ~10 messages are pending (claimed but not ACKed) at any time
- Reclaimer easily handles this with COUNT=100 (recovers all stale entries in 1 call)
- **Catastrophic failure scenario** (e.g., all nodes drain): up to 10k messages become pending
  - 10k ÷ 100 = 100 iterations needed
  - 100 iterations × 30s = **50 minutes to fully recover** (only if all work is stale)
  - In practice, only the parallelism-bound messages (e.g., 10-200) go stale simultaneously

**Impact at higher parallelism** (e.g., parallelism=200):
- Normal operation: ~200 pending messages → recovered in 2 iterations (1 minute)
- Catastrophic failure: 10k pending → 100 iterations (50 minutes)

**Real-world impact**: **Low to Medium**
- Reclaimer only matters when pods die without completing (node failures, OOMKills, preemptions)
- In healthy operation, most messages are ACKed immediately by `handleCompletedPods`
- Stale pending count is typically << parallelism (a few pods, not hundreds)
- 50-minute recovery only happens if the entire queue gets stuck (rare)

**Mitigations**:
- **Increase COUNT to 500** (recovers 10k in 20 iterations = 10 min worst case)
- Run reclaimer more frequently when `pendingCount > parallelism * 2` (adaptive)
- For most workloads, current design is adequate

#### 5. **Job Parallelism vs Cluster Autoscaler**
**Current behavior**: Job parallelism defaults to 10, max user-configurable.

**Impact at high parallelism** (e.g., 200):
- 200 pods request resources simultaneously
- Cluster autoscaler may take 2-5 minutes to provision nodes
- 200 pending pods → scheduler evaluates 200 pods per cycle

**Recommendations**:
- User controls parallelism (good)
- Document recommended parallelism: 10-50 for most workloads
- Consider adaptive parallelism: controller gradually increases based on cluster capacity
- Monitor cluster autoscaler lag

### Performance Characteristics (10k files, parallelism=10)

| Metric | Current Design | Notes |
|--------|----------------|-------|
| **Initialization** | 15-45s (list + 10k XADDs) | One-time cost, could optimize with pipelining |
| **Active Pods** | ~10-20 at any time | Bounded by parallelism |
| **Pod Patch Requests** | ~10/30s steady state | Natural rate limiting via parallelism |
| **Pod List Size** | 50-100KB per reconciliation | Only active pods, not all 10k |
| **Reclaimer (normal)** | <1 minute | Handles ~10 stale messages easily |
| **Reclaimer (catastrophic)** | 50 min worst case | Only if entire queue stalls (rare) |
| **API Server Load** | **Low** | Parallelism naturally limits load |
| **Valkey Memory** | 10-50MB per run | For 10k messages + metadata |
| **Controller Memory** | <5MB per run | Small pod cache due to parallelism |

### Performance Characteristics (10k files, parallelism=200)

| Metric | High Parallelism | Notes |
|--------|------------------|-------|
| **Initialization** | 15-45s | Same (one-time cost) |
| **Active Pods** | ~200-400 at any time | Bounded by parallelism |
| **Pod Patch Requests** | ~200 during startup burst | Manageable for most API servers |
| **Pod List Size** | 1-2MB per reconciliation | Still reasonable |
| **Reclaimer (normal)** | ~1 minute (2 iterations) | Handles ~200 stale messages |
| **Reclaimer (catastrophic)** | 50 min worst case | Only if entire queue stalls |
| **API Server Load** | **Moderate** | Higher but still bounded |
| **Cluster Autoscaler** | 2-5 min to provision nodes | May need tuning |

### Recommendations for 10k+ Scale

**High Priority** (Nice to have, not critical):
1. **Pipeline XADD calls** during initialization (reduce 15-45s → 5-10s)
2. **Increase XAUTOCLAIM COUNT to 500** (reduce catastrophic recovery from 50 min → 10 min)

**Low Priority** (Optional optimizations):
1. Document recommended parallelism (10-50 for most workloads)
2. Add retry + jitter to claimer pod annotation patching (already has 10s timeout)
3. Monitor API server metrics to validate low load
4. Consider adaptive reclaimer (increase frequency when `pendingCount > parallelism * 2`)

**NOT Needed for 10k Scale**:
- Pod listing optimization (already fast due to parallelism limiting)
- Alternative to pod annotations (not a bottleneck)
- Horizontal controller scaling (single controller handles this easily)

**For 100k+ files**:
1. Pipeline XADD calls become more important (45s → 10s initialization)
2. Dedicated Valkey cluster (separate from other workloads)
3. Increase Valkey memory limits (100-500MB per run)
4. Consider chunking: split large runs into multiple smaller PipelineRuns (e.g., 5 runs of 20k files)
5. May need horizontal controller scaling at very high concurrency (multiple large runs)

### Current Design Verdict

**Can handle 10k files**: **YES - Design is Excellent for This Scale**
- Initialization: 15-45s (acceptable, one-time cost)
- Runtime API server load: **Low** (parallelism naturally limits load)
- Pod management: **Efficient** (only 10-200 active pods, not 10k)
- Memory usage: **Minimal** (<5MB controller memory per run at parallelism=10)
- Reclaimer: Works well in normal operation; 50-min recovery only in catastrophic failures (rare)
- **No critical issues identified**

**Key insight**: Parallelism is the critical design element that makes this scale so well. By processing 10-200 files concurrently (not 10k concurrently), the design naturally limits:
- Active pod count
- API server request rate
- Controller memory usage
- Pending message count (and thus reclaimer load)

**Can handle 100k files**: **YES - Same Design Scales**
- Initialization: 45s → 10s with pipelined XADDs (recommended)
- Runtime behavior: Identical to 10k (still only parallelism-many active pods)
- Valkey memory: ~100-500MB (need to size appropriately)
- May want to chunk into multiple 20k-file runs for operational convenience

## 12. Testing Strategy

### Unit

- API handler (S3 paging, XADD calls)
- Claimer (parsing, annotation patch)
- Reconciler (ACK/retry)
- Reclaimer

### Integration (kind/minio/valkey)

- **Small run:** 100 files, parallelism=20, inject 5% failures → verify retries/DLQ
- **Crash resilience:** kill nodes mid-run → ensure XAUTOCLAIM recovery
- **Scale:** 50k messages, observe stable API server usage and Job completion

### E2E

- Happy path, DLQ path, idempotent reruns (same runId not reused)

## 13. Rollout Plan

1. Deploy Valkey (HA) and MinIO (if not using cloud S3); create valkey-creds secret
2. Deploy controller + CRDs
3. Dry-run with test pipeline (tiny filters) and small bucket
4. Gradually raise parallelism while watching cluster utilization and Valkey metrics
5. Enable autoscaling (nodes) and adjust resource limits per filter
6. Add alerts on DLQ growth, high retries, long pending entries

## 14. Open Questions

- Should we persist a run manifest (JSONL) to the bucket summarizing outcomes for audit?
- Do we need output configuration (e.g., destination bucket for processed files)?
- Should we support file filtering by suffix/pattern in the ObjectStorageSource spec?
