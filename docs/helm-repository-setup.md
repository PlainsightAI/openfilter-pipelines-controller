# Helm Repository Setup with GitHub Pages

This document describes how the Helm chart repository is configured using GitHub Pages.

## Overview

The project uses GitHub Pages to host a Helm chart repository at `https://plainsightai.github.io/openfilter-pipelines-controller`. The chart is automatically packaged and published when changes are merged to the `main` branch.

## How It Works

### Automated Release Process

1. When a new version tag (e.g., `v1.0.0`) is pushed:
   - The `update-helm-version.yml` workflow creates a PR to update `Chart.yaml` with the new version
   - The PR auto-merges once checks pass

2. When the version update is merged to `main`:
   - The `helm-release.yml` workflow is triggered by changes to `Chart.yaml`
   - The workflow uses [chart-releaser-action](https://github.com/helm/chart-releaser-action) to:
     - Package the Helm chart
     - Create a GitHub Release with the packaged chart (.tgz file)
     - Update the `index.yaml` file in the `gh-pages` branch
     - Commit and push changes to the `gh-pages` branch

3. GitHub Pages serves the static files from the `gh-pages` branch

### Chart Releaser Action

The [helm/chart-releaser-action](https://github.com/helm/chart-releaser-action) handles:
- Automatic chart packaging with `helm package`
- Creating GitHub Releases for each chart version
- Managing the Helm repository index (`index.yaml`)
- Pushing updates to the `gh-pages` branch

## Initial Setup

### 1. Enable GitHub Pages

In the repository settings (https://github.com/PlainsightAI/openfilter-pipelines-controller/settings/pages):

1. Go to **Settings** > **Pages**
2. Under **Source**, select:
   - **Branch**: `gh-pages`
   - **Folder**: `/ (root)`
3. Click **Save**

GitHub will automatically create the `gh-pages` branch on the first workflow run if it doesn't exist.

### 2. Verify Workflow Permissions

Ensure the workflow has write permissions:

1. Go to **Settings** > **Actions** > **General**
2. Under **Workflow permissions**, select:
   - **Read and write permissions**
3. Check **Allow GitHub Actions to create and approve pull requests**
4. Click **Save**

## Testing the Setup

### Verify Chart Publication

1. After pushing a version tag and merging the version update:
   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

2. Check the workflow runs:
   - Go to **Actions** tab in GitHub
   - Verify both `Update Helm Chart Version` and `Release Helm Chart` workflows succeed

3. Check the `gh-pages` branch:
   ```sh
   git fetch origin gh-pages
   git checkout gh-pages
   ls -la
   # Should see: index.yaml, openfilter-pipelines-controller-0.1.0.tgz, etc.
   ```

4. Verify GitHub Releases:
   - Go to **Releases** in the repository
   - Should see a release named `openfilter-pipelines-controller-0.1.0`

### Test Installing the Chart

```sh
# Add the repository
helm repo add openfilter-pipelines https://plainsightai.github.io/openfilter-pipelines-controller
helm repo update

# Search for the chart
helm search repo openfilter-pipelines

# Install the chart
helm install test-install openfilter-pipelines/openfilter-pipelines-controller \
  --namespace test-system \
  --create-namespace \
  --dry-run
```

## Maintenance

### Releasing a New Chart Version

1. Create and push a version tag:
   ```sh
   git tag v0.2.0
   git push origin v0.2.0
   ```

2. The automation will:
   - Update `Chart.yaml` via PR
   - Auto-merge the PR
   - Package and publish the chart
   - Update the Helm repository index

### Manual Chart Release (Emergency)

If the automation fails, you can manually release:

```sh
# Install chart-releaser CLI
brew install chart-releaser

# Package the chart
helm package charts/openfilter-pipelines-controller

# Create GitHub Release and update index
cr upload -o PlainsightAI -r openfilter-pipelines-controller \
  -p openfilter-pipelines-controller-0.1.0.tgz

cr index -o PlainsightAI -r openfilter-pipelines-controller \
  -c https://plainsightai.github.io/openfilter-pipelines-controller \
  --push
```

## Troubleshooting

### Chart Not Appearing in Repository

1. Verify the `gh-pages` branch exists and has content
2. Check GitHub Pages is enabled in repository settings
3. Verify workflow completed successfully
4. Check GitHub Release was created
5. Clear Helm cache: `helm repo remove openfilter-pipelines && helm repo add openfilter-pipelines https://plainsightai.github.io/openfilter-pipelines-controller`

### Workflow Permission Issues

If the workflow fails with permission errors:
1. Check workflow permissions in repository settings
2. Ensure `GITHUB_TOKEN` has `contents: write` permission
3. Verify the repository is not using a restrictive organization-level policy

### Index.yaml Not Updating

1. Check the `helm-release.yml` workflow logs
2. Verify chart version was incremented (chart-releaser skips existing versions)
3. Manually inspect the `gh-pages` branch for the `index.yaml` file

## References

- [Helm Chart Repository Guide](https://helm.sh/docs/topics/chart_repository/)
- [Chart Releaser Action](https://github.com/helm/chart-releaser-action)
- [GitHub Pages Documentation](https://docs.github.com/en/pages)
