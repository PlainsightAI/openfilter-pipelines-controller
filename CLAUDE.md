# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Kubernetes operator built with Kubebuilder v4 that manages Pipeline custom resources. The operator runs in a Kubernetes cluster and reconciles `Pipeline` CRDs in the `pipelines.plainsight.ai` API group.

**Domain**: plainsight.ai
**API Group**: pipelines.plainsight.ai
**Current Version**: v1alpha1
**Repository**: github.com/PlainsightAI/openfilter-pipelines-runner

## Development Commands

### Build and Run
```bash
# Build the manager binary
make build

# Run the controller locally (against configured cluster)
make run

# Format code
make fmt

# Run go vet
make vet
```

### Testing
```bash
# Run unit tests (auto-runs manifests, generate, fmt, vet first)
make test

# Run e2e tests (creates Kind cluster, runs tests, cleans up)
make test-e2e

# Set up e2e test cluster only (without running tests)
make setup-test-e2e

# Clean up e2e test cluster
make cleanup-test-e2e
```

### Linting
```bash
# Run golangci-lint
make lint

# Run golangci-lint with auto-fixes
make lint-fix

# Verify linter configuration
make lint-config
```

### Code Generation
```bash
# Generate CRD manifests, RBAC, webhooks
make manifests

# Generate DeepCopy methods
make generate
```

### Cluster Operations
```bash
# Install CRDs into cluster
make install

# Deploy controller to cluster
make deploy IMG=<registry>/openfilter-pipelines-runner:tag

# Create sample Pipeline resources
kubectl apply -k config/samples/

# Uninstall CRDs from cluster
make uninstall

# Remove controller from cluster
make undeploy
```

### Docker/Container
```bash
# Build Docker image
make docker-build IMG=<registry>/openfilter-pipelines-runner:tag

# Push Docker image
make docker-push IMG=<registry>/openfilter-pipelines-runner:tag

# Build and push multi-platform images
make docker-buildx IMG=<registry>/openfilter-pipelines-runner:tag
```

### Distribution
```bash
# Generate consolidated install.yaml in dist/
make build-installer IMG=<registry>/openfilter-pipelines-runner:tag
```

## Architecture

### Directory Structure

- **api/v1alpha1/**: Pipeline CRD type definitions
  - `pipeline_types.go`: Defines PipelineSpec, PipelineStatus, Pipeline, and PipelineList
  - Generated code includes DeepCopy methods and scheme registration

- **internal/controller/**: Controller reconciliation logic
  - `pipeline_controller.go`: Main reconciliation loop for Pipeline resources
  - Currently scaffolded with empty reconciliation logic

- **cmd/main.go**: Manager entry point
  - Sets up controller-runtime manager
  - Configures metrics server (HTTPS by default on :8443)
  - Configures webhook server
  - Handles leader election
  - HTTP/2 disabled by default for security

- **config/**: Kustomize manifests for deployment
  - `config/rbac/`: RBAC roles and bindings
  - `config/crd/`: CRD manifests (generated from api/)
  - `config/default/`: Default kustomize config
  - `config/samples/`: Example Pipeline CR manifests
  - `config/manager/`: Controller deployment manifest

- **test/**: Test utilities and e2e tests
  - `test/e2e/`: End-to-end test suite using Ginkgo
  - `test/utils/`: Test utilities

### Controller Pattern

This operator follows the standard Kubernetes operator pattern:

1. **Reconciliation Loop**: The `PipelineReconciler.Reconcile()` method is called whenever:
   - A Pipeline resource is created, updated, or deleted
   - The reconciler is restarted
   - A periodic resync occurs

2. **Status Management**: Pipeline resources have a status subresource with conditions following Kubernetes API conventions (Available, Progressing, Degraded)

3. **RBAC**: Controller has permissions on Pipeline resources via kubebuilder RBAC markers in controller code

### Code Generation Workflow

When modifying CRD types or controller RBAC:

1. Edit `api/v1alpha1/pipeline_types.go` for spec/status changes
2. Add/modify kubebuilder markers (comments starting with `+kubebuilder:`)
3. Run `make manifests` to regenerate CRDs and RBAC
4. Run `make generate` to regenerate DeepCopy methods
5. Run `make` or `make build` to compile (auto-runs manifests, generate, fmt, vet)

## Key Configuration

- **Go version**: 1.25.1
- **Kubernetes version**: 1.11.3+ (for deployment)
- **Controller-runtime**: v0.22.1
- **Kubebuilder**: v4.9.0
- **Leader Election ID**: 443c3c67.plainsight.ai

## Testing Notes

- Unit tests use envtest to simulate a Kubernetes API server
- E2e tests create a temporary Kind cluster named `openfilter-pipelines-runner-test-e2e`
- Kind must be pre-installed for e2e tests
- Test framework: Ginkgo v2 with Gomega matchers

## Linter Configuration

The project uses golangci-lint v2.4.0 with the following enabled linters:
- copyloopvar, dupl, errcheck, ginkgolinter, goconst, gocyclo, govet
- ineffassign, lll, misspell, nakedret, prealloc, revive, staticcheck
- unconvert, unparam, unused

Line length checks (lll) are relaxed for api/ and internal/ directories.
