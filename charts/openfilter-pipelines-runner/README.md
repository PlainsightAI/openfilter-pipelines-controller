# OpenFilter Pipelines Runner Helm Chart

A Kubernetes operator for managing Pipeline custom resources in the `filter.plainsight.ai` API group.

## Prerequisites

- Kubernetes 1.11.3+
- Helm 3.0+

## Installation

### Add the Helm repository (if published)

```bash
helm repo add plainsight https://charts.plainsight.ai
helm repo update
```

### Install from local chart

```bash
# Install with default values
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner

# Install with custom namespace
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --namespace pipelines-system \
  --create-namespace

# Install with Valkey enabled
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --set valkey.enabled=true
```

## Configuration

The following table lists the main configurable parameters of the chart and their default values.

### Controller Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of controller replicas | `1` |
| `image.repository` | Controller image repository | `ghcr.io/plainsightai/openfilter-pipelines-runner` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `image.tag` | Image tag (defaults to chart appVersion) | `""` |

### RBAC Configuration

**Controller RBAC**: Permissions for the controller manager to manage Pipeline resources.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `rbac.create` | Create RBAC resources for controller | `true` |
| `serviceAccount.create` | Create service account for controller | `true` |
| `serviceAccount.name` | Service account name for controller | `""` |

**Pipeline Executor RBAC**: Permissions for pods created by the controller (pipeline executor pods).

The controller creates pods that execute pipeline workloads. These pods **require** a service account with permission to patch their own annotations (used by the claimer init container to store queue metadata). This service account is always created and cannot be disabled.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `pipelineExecutor.serviceAccount.name` | Service account name for executor pods | `"pipeline-exec"` |
| `pipelineExecutor.serviceAccount.automount` | Automount service account token | `true` |
| `pipelineExecutor.serviceAccount.annotations` | Annotations for the service account | `{}` |

### CRD Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `crds.install` | Install CRDs with the chart | `true` |
| `crds.keep` | Keep CRDs on chart uninstall | `true` |

### Controller Settings

| Parameter | Description | Default |
|-----------|-------------|---------|
| `controller.leaderElection.enabled` | Enable leader election | `true` |
| `controller.leaderElection.id` | Leader election ID | `443c3c67.plainsight.ai` |
| `controller.metrics.enabled` | Enable metrics server | `true` |
| `controller.metrics.port` | Metrics server port | `8443` |
| `controller.metrics.secureServing` | Use secure metrics serving | `true` |
| `controller.health.port` | Health probe port | `8081` |
| `controller.webhook.enabled` | Enable webhook server | `false` |

### Resources

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.limits.cpu` | CPU limit | `500m` |
| `resources.limits.memory` | Memory limit | `128Mi` |
| `resources.requests.cpu` | CPU request | `10m` |
| `resources.requests.memory` | Memory request | `64Mi` |

### Valkey Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `valkey.enabled` | Enable Valkey installation | `false` |
| `valkey.architecture` | Valkey architecture (standalone/replication) | `standalone` |
| `valkey.auth.enabled` | Enable authentication | `true` |
| `valkey.auth.password` | Password (auto-generated if empty) | `""` |
| `valkey.primary.persistence.enabled` | Enable persistence | `true` |
| `valkey.primary.persistence.size` | Persistent volume size | `8Gi` |

For a complete list of Valkey parameters, see the [Bitnami Valkey chart documentation](https://artifacthub.io/packages/helm/bitnami/valkey).

## Examples

### Install with custom resources

```bash
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --set resources.limits.cpu=1000m \
  --set resources.limits.memory=256Mi \
  --set resources.requests.cpu=100m \
  --set resources.requests.memory=128Mi
```

### Install with Valkey in replication mode

```bash
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --set valkey.enabled=true \
  --set valkey.architecture=replication \
  --set valkey.replica.replicaCount=3
```

### Install with custom image

```bash
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --set image.repository=myregistry.io/openfilter-pipelines-runner \
  --set image.tag=v1.0.0 \
  --set image.pullPolicy=Always
```

## Upgrading

```bash
# Upgrade to a new version
helm upgrade openfilter-pipelines-runner charts/openfilter-pipelines-runner

# Upgrade with new values
helm upgrade openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --set controller.metrics.enabled=false
```

## Uninstalling

```bash
helm uninstall openfilter-pipelines-runner
```

**Note:** By default, CRDs are kept on uninstall to prevent data loss. To remove CRDs manually:

```bash
kubectl delete crd filter.plainsight.ai
kubectl delete crd pipelineruns.plainsight.ai
```

## Development

### Update CRDs in the chart

When you modify the API types (in `api/v1alpha1/`) and regenerate CRDs, you need to sync them to the Helm chart:

```bash
# From the project root
make helm-update-crds
```

This will:
1. Run `make manifests` to regenerate CRDs in `config/crd/bases/`
2. Copy the updated CRDs to `charts/openfilter-pipelines-runner/crds/`
3. Show git status of the changes

**Manual sync alternative:**

```bash
# Generate CRDs
make manifests

# Run the sync script directly
./hack/update-helm-crds.sh
```

**CI Integration:**

The project includes a GitHub Actions workflow (`.github/workflows/helm-crd-sync-check.yml`) that automatically verifies CRDs are in sync on pull requests. If CRDs are out of sync, the CI check will fail.

### Update dependencies

```bash
cd charts/openfilter-pipelines-runner
helm dependency update
```

### Package the chart

```bash
helm package charts/openfilter-pipelines-runner
```

### Test the chart locally

```bash
# Template the chart to see rendered manifests
helm template openfilter-pipelines-runner charts/openfilter-pipelines-runner

# Test installation with dry-run
helm install openfilter-pipelines-runner charts/openfilter-pipelines-runner \
  --dry-run --debug
```

## License

See the [LICENSE](../../LICENSE) file for details.

## Support

For issues and questions, please visit: https://github.com/PlainsightAI/openfilter-pipelines-runner/issues
