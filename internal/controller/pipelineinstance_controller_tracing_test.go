package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

const (
	testTraceparent      = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	testTracestate       = "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"
	testExporterType     = "otlp_grpc"
	testExporterEndpoint = "otel-collector.monitoring.svc.cluster.local:4317"
)

// makeTracingFilterPipeline returns a Pipeline with a single CPU filter so the
// tracing-injection paths can be exercised without bringing GPU branching into
// the picture.
func makeTracingFilterPipeline() *pipelinesv1alpha1.Pipeline {
	return &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "trace-filter",
					Image: "filter:latest",
				},
			},
		},
	}
}

// ─── batch (Job) ─────────────────────────────────────────────────────────────

func TestBuildJob_TraceparentInjectedFromAnnotation(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
	}
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	tp, ok := findEnvVar(env, "TRACEPARENT")
	if !ok {
		t.Fatal("expected TRACEPARENT env var to be set when annotation is present")
	}
	if tp.Value != testTraceparent {
		t.Errorf("expected TRACEPARENT=%q, got %q", testTraceparent, tp.Value)
	}
}

func TestBuildJob_TraceparentNotInjectedWithoutAnnotation(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	// no annotations on the PipelineInstance — TRACEPARENT must NOT be set,
	// otherwise filter pods would inherit a stale parent context from a
	// previous run and merge unrelated spans into the same trace.
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACEPARENT"); ok {
		t.Error("expected TRACEPARENT NOT to be set when annotation is absent")
	}
}

func TestBuildJob_TraceparentNotInjectedForEmptyAnnotationValue(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: "",
	}
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACEPARENT"); ok {
		t.Error("expected TRACEPARENT NOT to be set when annotation value is empty")
	}
}

func TestBuildJob_TracestateInjectedFromAnnotation(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
		TracestateAnnotation:  testTracestate,
	}
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	ts, ok := findEnvVar(env, "TRACESTATE")
	if !ok {
		t.Fatal("expected TRACESTATE env var to be set when annotation is present")
	}
	if ts.Value != testTracestate {
		t.Errorf("expected TRACESTATE=%q, got %q", testTracestate, ts.Value)
	}
}

func TestBuildJob_TracestateNotInjectedWithoutAnnotation(t *testing.T) {
	// Only traceparent is set — the upstream chain has no vendor tracestate
	// to propagate. TRACESTATE must be omitted entirely so we don't shadow
	// any default the runtime would otherwise infer.
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
	}
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACEPARENT"); !ok {
		t.Fatal("precondition: expected TRACEPARENT to be set in this test")
	}
	if _, ok := findEnvVar(env, "TRACESTATE"); ok {
		t.Error("expected TRACESTATE NOT to be set when tracestate annotation is absent")
	}
}

func TestBuildJob_TracestateNotInjectedForEmptyAnnotationValue(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
		TracestateAnnotation:  "",
	}
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACESTATE"); ok {
		t.Error("expected TRACESTATE NOT to be set when annotation value is empty")
	}
}

func TestBuildJob_TracestateNotInjectedWithoutTraceparent(t *testing.T) {
	// Tracestate is meaningless without a parent context for the W3C
	// propagator to interpret it against. If only TracestateAnnotation is
	// set (no TraceparentAnnotation), the controller MUST drop tracestate
	// rather than inject TRACESTATE alone — otherwise the comment in
	// tracingEnvVars (claiming tracestate rides with traceparent) becomes a
	// lie and downstream observability tools see orphaned vendor state.
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Annotations = map[string]string{
		TracestateAnnotation: testTracestate,
	}
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACEPARENT"); ok {
		t.Error("expected TRACEPARENT NOT to be set when only tracestate annotation is present")
	}
	if _, ok := findEnvVar(env, "TRACESTATE"); ok {
		t.Error("expected TRACESTATE NOT to be set when traceparent annotation is absent (tracestate without parent context is meaningless)")
	}
}

func TestBuildJob_NeitherTracingAnnotationInjectsNothing(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	// no tracing annotations at all
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACEPARENT"); ok {
		t.Error("expected TRACEPARENT NOT to be set when no tracing annotations are present")
	}
	if _, ok := findEnvVar(env, "TRACESTATE"); ok {
		t.Error("expected TRACESTATE NOT to be set when no tracing annotations are present")
	}
}

func TestBuildJob_TelemetryExporterEnvInjectedWhenReconcilerConfigured(t *testing.T) {
	r := makeMinimalReconciler()
	r.TelemetryExporterType = testExporterType
	r.TelemetryExporterOTLPEndpoint = testExporterEndpoint
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env

	typ, ok := findEnvVar(env, "TELEMETRY_EXPORTER_TYPE")
	if !ok {
		t.Fatal("expected TELEMETRY_EXPORTER_TYPE to be set")
	}
	if typ.Value != testExporterType {
		t.Errorf("expected TELEMETRY_EXPORTER_TYPE=%q, got %q", testExporterType, typ.Value)
	}

	ep, ok := findEnvVar(env, "TELEMETRY_EXPORTER_OTLP_ENDPOINT")
	if !ok {
		t.Fatal("expected TELEMETRY_EXPORTER_OTLP_ENDPOINT to be set")
	}
	if ep.Value != testExporterEndpoint {
		t.Errorf("expected TELEMETRY_EXPORTER_OTLP_ENDPOINT=%q, got %q", testExporterEndpoint, ep.Value)
	}
}

func TestBuildJob_TelemetryExporterEnvNotInjectedByDefault(t *testing.T) {
	// OSS-friendly contract: when the operator hasn't set the controller
	// flags, filter pods MUST NOT have the env vars at all (so openfilter
	// stays in its silent-exporter default and never tries to ship spans
	// to a collector that may not exist).
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TELEMETRY_EXPORTER_TYPE"); ok {
		t.Error("expected TELEMETRY_EXPORTER_TYPE NOT to be set when reconciler field is empty")
	}
	if _, ok := findEnvVar(env, "TELEMETRY_EXPORTER_OTLP_ENDPOINT"); ok {
		t.Error("expected TELEMETRY_EXPORTER_OTLP_ENDPOINT NOT to be set when reconciler field is empty")
	}
}

func TestBuildJob_AllTracingEnvCombined(t *testing.T) {
	// The full Plainsight production combination: controller has been
	// configured with both exporter flags AND the reconciled
	// PipelineInstance carries a traceparent annotation. The filter
	// container env must include all three so the openfilter SDK can
	// (a) ship spans, and (b) attach them to the parent trace.
	r := makeMinimalReconciler()
	r.TelemetryExporterType = testExporterType
	r.TelemetryExporterOTLPEndpoint = testExporterEndpoint
	pi := makeMinimalPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
		TracestateAnnotation:  testTracestate,
	}
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env

	for _, want := range []struct {
		name  string
		value string
	}{
		{"TRACEPARENT", testTraceparent},
		{"TRACESTATE", testTracestate},
		{"TELEMETRY_EXPORTER_TYPE", testExporterType},
		{"TELEMETRY_EXPORTER_OTLP_ENDPOINT", testExporterEndpoint},
	} {
		got, ok := findEnvVar(env, want.name)
		if !ok {
			t.Errorf("expected %s to be set, was not", want.name)
			continue
		}
		if got.Value != want.value {
			t.Errorf("expected %s=%q, got %q", want.name, want.value, got.Value)
		}
	}
}

// ─── streaming (Deployment) ──────────────────────────────────────────────────

func TestBuildStreamingDeployment_TraceparentInjectedFromAnnotation(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
	}

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")

	env := deployment.Spec.Template.Spec.Containers[0].Env
	tp, ok := findEnvVar(env, "TRACEPARENT")
	if !ok {
		t.Fatal("expected TRACEPARENT env var to be set when annotation is present")
	}
	if tp.Value != testTraceparent {
		t.Errorf("expected TRACEPARENT=%q, got %q", testTraceparent, tp.Value)
	}
}

func TestBuildStreamingDeployment_TracestateInjectedFromAnnotation(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
		TracestateAnnotation:  testTracestate,
	}

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")

	env := deployment.Spec.Template.Spec.Containers[0].Env
	ts, ok := findEnvVar(env, "TRACESTATE")
	if !ok {
		t.Fatal("expected TRACESTATE env var to be set when annotation is present")
	}
	if ts.Value != testTracestate {
		t.Errorf("expected TRACESTATE=%q, got %q", testTracestate, ts.Value)
	}
}

func TestBuildStreamingDeployment_TracestateNotInjectedWithoutAnnotation(t *testing.T) {
	// Only traceparent is set — TRACESTATE must be omitted (not injected as
	// empty), so the runtime's default tracestate handling is preserved.
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
	}

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")

	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACEPARENT"); !ok {
		t.Fatal("precondition: expected TRACEPARENT to be set in this test")
	}
	if _, ok := findEnvVar(env, "TRACESTATE"); ok {
		t.Error("expected TRACESTATE NOT to be set when tracestate annotation is absent")
	}
}

func TestBuildStreamingDeployment_TracestateNotInjectedForEmptyAnnotationValue(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
		TracestateAnnotation:  "",
	}

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")

	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACESTATE"); ok {
		t.Error("expected TRACESTATE NOT to be set when annotation value is empty")
	}
}

func TestBuildStreamingDeployment_TracestateNotInjectedWithoutTraceparent(t *testing.T) {
	// Mirror of the batch test: tracestate without traceparent is dropped.
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Annotations = map[string]string{
		TracestateAnnotation: testTracestate,
	}

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")

	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACEPARENT"); ok {
		t.Error("expected TRACEPARENT NOT to be set when only tracestate annotation is present")
	}
	if _, ok := findEnvVar(env, "TRACESTATE"); ok {
		t.Error("expected TRACESTATE NOT to be set when traceparent annotation is absent (tracestate without parent context is meaningless)")
	}
}

func TestBuildStreamingDeployment_NeitherTracingAnnotationInjectsNothing(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	// no tracing annotations at all

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")

	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TRACEPARENT"); ok {
		t.Error("expected TRACEPARENT NOT to be set when no tracing annotations are present")
	}
	if _, ok := findEnvVar(env, "TRACESTATE"); ok {
		t.Error("expected TRACESTATE NOT to be set when no tracing annotations are present")
	}
}

func TestBuildStreamingDeployment_TelemetryExporterEnvInjectedWhenReconcilerConfigured(t *testing.T) {
	r := &PipelineInstanceReconciler{
		TelemetryExporterType:         testExporterType,
		TelemetryExporterOTLPEndpoint: testExporterEndpoint,
	}
	pi := makeMinimalStreamingPipelineInstance()

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")

	env := deployment.Spec.Template.Spec.Containers[0].Env

	typ, ok := findEnvVar(env, "TELEMETRY_EXPORTER_TYPE")
	if !ok {
		t.Fatal("expected TELEMETRY_EXPORTER_TYPE to be set")
	}
	if typ.Value != testExporterType {
		t.Errorf("expected TELEMETRY_EXPORTER_TYPE=%q, got %q", testExporterType, typ.Value)
	}

	ep, ok := findEnvVar(env, "TELEMETRY_EXPORTER_OTLP_ENDPOINT")
	if !ok {
		t.Fatal("expected TELEMETRY_EXPORTER_OTLP_ENDPOINT to be set")
	}
	if ep.Value != testExporterEndpoint {
		t.Errorf("expected TELEMETRY_EXPORTER_OTLP_ENDPOINT=%q, got %q", testExporterEndpoint, ep.Value)
	}
}

func TestBuildStreamingDeployment_TelemetryExporterEnvNotInjectedByDefault(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")

	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, "TELEMETRY_EXPORTER_TYPE"); ok {
		t.Error("expected TELEMETRY_EXPORTER_TYPE NOT to be set when reconciler field is empty")
	}
	if _, ok := findEnvVar(env, "TELEMETRY_EXPORTER_OTLP_ENDPOINT"); ok {
		t.Error("expected TELEMETRY_EXPORTER_OTLP_ENDPOINT NOT to be set when reconciler field is empty")
	}
}

func TestBuildStreamingDeployment_AllTracingEnvCombined(t *testing.T) {
	r := &PipelineInstanceReconciler{
		TelemetryExporterType:         testExporterType,
		TelemetryExporterOTLPEndpoint: testExporterEndpoint,
	}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
		TracestateAnnotation:  testTracestate,
	}

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	want := map[string]string{
		"TRACEPARENT":                      testTraceparent,
		"TRACESTATE":                       testTracestate,
		"TELEMETRY_EXPORTER_TYPE":          testExporterType,
		"TELEMETRY_EXPORTER_OTLP_ENDPOINT": testExporterEndpoint,
	}
	for name, value := range want {
		got, ok := findEnvVar(env, name)
		if !ok {
			t.Errorf("expected %s to be set, was not", name)
			continue
		}
		if got.Value != value {
			t.Errorf("expected %s=%q, got %q", name, value, got.Value)
		}
	}
}

// Defensive: ensure that injecting tracing env vars never clobbers
// user-defined Filter.Env entries that already happen to use the same names.
// (Our injection happens before filter.Env is appended, so user values win.)
func TestBuildJob_FilterEnvOverridesTracingEnv(t *testing.T) {
	r := makeMinimalReconciler()
	r.TelemetryExporterType = testExporterType
	r.TelemetryExporterOTLPEndpoint = testExporterEndpoint
	pi := makeMinimalPipelineInstance()
	pi.Annotations = map[string]string{
		TraceparentAnnotation: testTraceparent,
	}
	ps := makeMinimalPipelineSource()

	const userEndpoint = "user-supplied:9999"
	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "trace-filter",
					Image: "filter:latest",
					Env: []corev1.EnvVar{
						{Name: "TELEMETRY_EXPORTER_OTLP_ENDPOINT", Value: userEndpoint},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env

	// Both entries will be present in the env list (Kubernetes does not dedupe);
	// what matters is that the user-supplied value appears AFTER the
	// controller-injected one. Per kubelet's effective-env construction: when
	// a Pod env list contains duplicate entries with the same Name, the last
	// one with that Name wins on the running container. (This is implementation
	// behavior of kubelet/CRI, not a documented API guarantee.)
	var sawController, sawUser bool
	var userIndex, controllerIndex int
	for i, e := range env {
		if e.Name != "TELEMETRY_EXPORTER_OTLP_ENDPOINT" {
			continue
		}
		switch e.Value {
		case userEndpoint:
			sawUser = true
			userIndex = i
		case testExporterEndpoint:
			sawController = true
			controllerIndex = i
		}
	}
	if !sawController || !sawUser {
		t.Fatalf("expected both controller (%v) and user (%v) entries to be present", sawController, sawUser)
	}
	if userIndex < controllerIndex {
		t.Errorf("expected user-supplied env to appear AFTER controller-injected env (controllerIndex=%d, userIndex=%d)", controllerIndex, userIndex)
	}
}

// PIPELINE_ID is NOT injected by this controller. On Plainsight clusters,
// plainsight-deployment-agent owns PIPELINE_ID and writes the canonical bare
// instance UUID directly into each filter's env (see
// plainsight-deployment-agent/internal/kubernetes/client.go). The controller
// would otherwise inject `pipelineInstance.Name` (e.g. "pi-<uuid>"), which
// disagrees with the agent's value and produces non-deterministic kubelet
// last-write-wins semantics. PIPELINE_INSTANCE_UID has no such conflict and
// IS always injected from CR metadata.
func TestBuildJob_PipelineIDNotInjectedByController(t *testing.T) {
	r := makeMinimalReconciler()
	// Deliberately leave all optional tracing knobs unset: no TRACEPARENT
	// annotation, no reconciler telemetry fields. PIPELINE_INSTANCE_UID
	// must still be present — it is unconditional from CR metadata.
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	job := r.buildJob(context.Background(), pi, makeTracingFilterPipeline(), ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env

	if _, ok := findEnvVar(env, "PIPELINE_ID"); ok {
		t.Error("expected PIPELINE_ID NOT to be injected by the controller; the deployment-agent owns this env var on Plainsight clusters")
	}

	uid, ok := findEnvVar(env, "PIPELINE_INSTANCE_UID")
	if !ok {
		t.Fatal("expected PIPELINE_INSTANCE_UID env var to always be injected")
	}
	if uid.Value != string(pi.UID) {
		t.Errorf("expected PIPELINE_INSTANCE_UID=%q, got %q", string(pi.UID), uid.Value)
	}

	// Sanity-check: the optional tracing vars really are absent in this config.
	if _, ok := findEnvVar(env, "TRACEPARENT"); ok {
		t.Error("TRACEPARENT should not be set in this test — helper assumption broken")
	}
	if _, ok := findEnvVar(env, "TELEMETRY_EXPORTER_TYPE"); ok {
		t.Error("TELEMETRY_EXPORTER_TYPE should not be set in this test — helper assumption broken")
	}
}

func TestBuildStreamingDeployment_PipelineIDNotInjectedByController(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()

	deployment := r.buildStreamingDeployment(pi, makeTracingFilterPipeline(), nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	if _, ok := findEnvVar(env, "PIPELINE_ID"); ok {
		t.Error("expected PIPELINE_ID NOT to be injected by the controller; the deployment-agent owns this env var on Plainsight clusters")
	}

	uid, ok := findEnvVar(env, "PIPELINE_INSTANCE_UID")
	if !ok {
		t.Fatal("expected PIPELINE_INSTANCE_UID env var to always be injected")
	}
	if uid.Value != string(pi.UID) {
		t.Errorf("expected PIPELINE_INSTANCE_UID=%q, got %q", string(pi.UID), uid.Value)
	}

	if _, ok := findEnvVar(env, "TRACEPARENT"); ok {
		t.Error("TRACEPARENT should not be set in this test — helper assumption broken")
	}
	if _, ok := findEnvVar(env, "TELEMETRY_EXPORTER_TYPE"); ok {
		t.Error("TELEMETRY_EXPORTER_TYPE should not be set in this test — helper assumption broken")
	}
}
