# Helm Charts

This directory contains Helm charts for deploying the OpenFilter Pipelines Runner.

## Available Charts

### openfilter-pipelines-controller

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
helm install openfilter-pipelines-controller charts/openfilter-pipelines-controller \
  --namespace pipelines-system \
  --create-namespace

# Install with Valkey enabled
helm install openfilter-pipelines-controller charts/openfilter-pipelines-controller \
  --namespace pipelines-system \
  --create-namespace \
  --set valkey.enabled=true
```

**Documentation:**

See [openfilter-pipelines-controller/README.md](./openfilter-pipelines-controller/README.md) for detailed configuration options and usage examples.

## Chart Structure

```
charts/
└── openfilter-pipelines-controller/
    ├── Chart.yaml                  # Chart metadata
    ├── values.yaml                 # Default values
    ├── values-production.yaml      # Production values example
    ├── README.md                   # Chart documentation
    ├── crds/                       # Custom Resource Definitions
    │   ├── filter.plainsight.ai_pipelines.yaml
    │   ├── filter.plainsight.ai_pipelinesources.yaml
    │   └── filter.plainsight.ai_pipelineinstances.yaml
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
cd charts/openfilter-pipelines-controller
helm lint .
```

### Testing

```bash
# Template the chart
helm template test-release charts/openfilter-pipelines-controller

# Dry-run installation
helm install test-release charts/openfilter-pipelines-controller --dry-run --debug

# Test with Valkey enabled
helm template test-release charts/openfilter-pipelines-controller --set valkey.enabled=true
```

### Updating Dependencies

```bash
cd charts/openfilter-pipelines-controller
helm dependency update
```

### Packaging

```bash
helm package charts/openfilter-pipelines-controller
```

## Publishing

To publish the chart to a Helm repository:

```bash
# Package the chart
helm package charts/openfilter-pipelines-controller

# Create or update the index
helm repo index .

# Upload the package and index to your chart repository
```

## Support

For issues and questions, please visit: https://github.com/PlainsightAI/openfilter-pipelines-controller/issues
