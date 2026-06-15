# OpenFilter Pipelines Runner Helm Chart

A Kubernetes operator for managing Pipeline custom resources in the `filter.plainsight.ai` API group.

## Prerequisites

- Kubernetes 1.11.3+
- Helm 3.0+

## Installation

### Install from GHCR (recommended)

The chart is published as an OCI artifact to GitHub Container Registry. Helm
3.8+ supports OCI directly — no `helm repo add` is needed:

```bash
helm install openfilter-pipelines-controller \
  oci://ghcr.io/plainsightai/charts/openfilter-pipelines-controller \
  --version <version>
```

The chart deliberately lives in a separate registry from the container images:
Docker Hub has a flat namespace, so the chart (whose OCI repository name is
derived from the chart name) would share a repository with the controller
image and overwrite the image manifest at each release tag.

Replace `<version>` with the desired release (e.g. `0.6.1`); the published
tags match the git tags on this repository (`vX.Y.Z` → chart `X.Y.Z`) and are
listed on the
[Releases](https://github.com/PlainsightAI/openfilter-pipelines-controller/releases)
page.

Container images:
[`plainsightai/openfilter-pipelines-controller`](https://hub.docker.com/r/plainsightai/openfilter-pipelines-controller)
and
[`plainsightai/openfilter-pipelines-claimer`](https://hub.docker.com/r/plainsightai/openfilter-pipelines-claimer).

### Install from a local chart copy

You can also install directly from this repository's local copy under
`deployment/openfilter-pipelines-controller`.

```bash
# Install with default values
helm install openfilter-pipelines-controller deployment/openfilter-pipelines-controller

# Install with custom namespace
helm install openfilter-pipelines-controller deployment/openfilter-pipelines-controller \
  --namespace pipelines-system \
  --create-namespace

# Install with Valkey enabled
helm install openfilter-pipelines-controller deployment/openfilter-pipelines-controller \
  --set valkey.enabled=true
```

## Configuration

The following table lists the main configurable parameters of the chart and their default values.

### Controller Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of controller replicas | `1` |
| `image.repository` | Controller image repository | `plainsightai/openfilter-pipelines-controller` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `image.tag` | Image tag (defaults to chart appVersion) | `""` |
| `claimerImage.repository` | Claimer image repository | `plainsightai/openfilter-pipelines-claimer` |
| `claimerImage.tag` | Claimer image tag (defaults to chart appVersion) | `""` |

### RBAC Configuration

**Controller RBAC**: Permissions for the controller manager to manage Pipeline resources.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `rbac.create` | Create RBAC resources for controller | `true` |
| `serviceAccount.create` | Create service account for controller | `true` |
| `serviceAccount.name` | Service account name for controller | `""` |

The chart no longer installs any per-workload ServiceAccounts for pipeline execution. Batch and streaming pods use the namespace default ServiceAccount.

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
helm install openfilter-pipelines-controller deployment/openfilter-pipelines-controller \
  --set resources.limits.cpu=1000m \
  --set resources.limits.memory=256Mi \
  --set resources.requests.cpu=100m \
  --set resources.requests.memory=128Mi
```

### Install with Valkey in replication mode

```bash
helm install openfilter-pipelines-controller deployment/openfilter-pipelines-controller \
  --set valkey.enabled=true \
  --set valkey.architecture=replication \
  --set valkey.replica.replicaCount=3
```

### Install with custom image

```bash
helm install openfilter-pipelines-controller deployment/openfilter-pipelines-controller \
  --set image.repository=myregistry.io/openfilter-pipelines-controller \
  --set image.tag=v1.0.0 \
  --set image.pullPolicy=Always
```

## Upgrading

```bash
# Upgrade to a new version
helm upgrade openfilter-pipelines-controller deployment/openfilter-pipelines-controller

# Upgrade with new values
helm upgrade openfilter-pipelines-controller deployment/openfilter-pipelines-controller \
  --set controller.metrics.enabled=false
```

## Uninstalling

```bash
helm uninstall openfilter-pipelines-controller
```

**Note:** By default, CRDs are kept on uninstall to prevent data loss. To remove CRDs manually:

```bash
kubectl delete crd pipelines.filter.plainsight.ai
kubectl delete crd pipelinesources.filter.plainsight.ai
kubectl delete crd pipelineinstances.filter.plainsight.ai
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
2. Copy the updated CRDs to `deployment/openfilter-pipelines-controller/crds/`
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
cd deployment/openfilter-pipelines-controller
helm dependency update
```

### Package the chart

```bash
helm package deployment/openfilter-pipelines-controller
```

### Test the chart locally

```bash
# Template the chart to see rendered manifests
helm template openfilter-pipelines-controller deployment/openfilter-pipelines-controller

# Test installation with dry-run
helm install openfilter-pipelines-controller deployment/openfilter-pipelines-controller \
  --dry-run --debug
```

## Releasing

Releases are driven entirely by git tags. To cut version `X.Y.Z`:

1. Bump `version` **and** `appVersion` in `Chart.yaml` to `X.Y.Z` in the same
   commit and merge it via PR. The `chart-version-parity` CI check
   (`.github/workflows/lint.yml`) keeps the two fields in lockstep and, on a
   release tag, asserts they match the tag — so a stale `Chart.yaml` fails CI
   before anything is published.
2. Tag the merged commit `vX.Y.Z` and push the tag.

On the tag push, `.github/workflows/docker-publish.yml`:

- builds and pushes the controller + claimer images to Docker Hub,
- packages the chart (with `--version`/`--app-version` derived from the tag)
  and pushes it to `oci://ghcr.io/plainsightai/charts`, and
- creates a GitHub Release for the tag with the packaged chart `.tgz` attached.

## License

See the [LICENSE](../../LICENSE) file for details.

## Support

For issues and questions, please visit: https://github.com/PlainsightAI/openfilter-pipelines-controller/issues
