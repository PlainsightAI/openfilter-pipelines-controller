# Helm Charts

This directory contains Helm charts for deploying the OpenFilter Pipelines Runner.

## Available Charts

### openfilter-pipelines-runner

The main Helm chart for deploying the Kubernetes operator that manages Pipeline custom resources.

**Features:**
- Automated CRD installation
- RBAC configuration
- Service account management
- Metrics and health endpoints
- Leader election support
- Optional Valkey (Redis alternative) subchart

**Quick Start:**

```bash
# Install with default values
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --namespace pipelines-system \
  --create-namespace

# Install with Valkey enabled
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --namespace pipelines-system \
  --create-namespace \
  --set valkey.enabled=true
```

**Documentation:**

See [openfilter-pipelines-runner/README.md](./openfilter-pipelines-runner/README.md) for detailed configuration options and usage examples.

## Chart Structure

```
charts/
└── openfilter-pipelines-runner/
    ├── Chart.yaml                  # Chart metadata
    ├── values.yaml                 # Default values
    ├── values-production.yaml      # Production values example
    ├── README.md                   # Chart documentation
    ├── crds/                       # Custom Resource Definitions
    │   ├── pipelines.plainsight.ai_pipelines.yaml
    │   └── pipelines.plainsight.ai_pipelineruns.yaml
    ├── templates/                  # Kubernetes manifests
    │   ├── deployment.yaml         # Controller deployment
    │   ├── service.yaml            # Services
    │   ├── serviceaccount.yaml     # Service account
    │   ├── rbac.yaml               # RBAC resources
    │   ├── NOTES.txt               # Post-install notes
    │   └── _helpers.tpl            # Template helpers
    └── charts/                     # Chart dependencies
        └── valkey-1.0.2.tgz        # Valkey subchart
```

## Development

### Prerequisites

- Helm 3.0+
- Kubernetes cluster (for testing)

### Linting

```bash
cd charts/openfilter-pipelines-runner
helm lint .
```

### Testing

```bash
# Template the chart
helm template test-release charts/openfilter-pipelines-runner

# Dry-run installation
helm install test-release charts/openfilter-pipelines-runner --dry-run --debug

# Test with Valkey enabled
helm template test-release charts/openfilter-pipelines-runner --set valkey.enabled=true
```

### Updating Dependencies

```bash
cd charts/openfilter-pipelines-runner
helm dependency update
```

### Packaging

```bash
helm package charts/openfilter-pipelines-runner
```

## Publishing

To publish the chart to a Helm repository:

```bash
# Package the chart
helm package charts/openfilter-pipelines-runner

# Create or update the index
helm repo index .

# Upload the package and index to your chart repository
```

## Support

For issues and questions, please visit: https://github.com/PlainsightAI/openfilter-pipelines-runner/issues
