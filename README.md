# openfilter-pipelines-controller

A Kubernetes operator built with Kubebuilder v4 that manages and reconciles Openfilter Pipeline custom resources in the `filter.plainsight.ai` API group.

## Description

This operator provides a Kubernetes-native way to define and manage data processing Openfilter Pipelines through the `Pipeline` custom resource definition (CRD). It runs within a Kubernetes cluster and automatically reconciles Pipeline resources, implementing the declarative configuration model for defining, deploying, and managing filter-based data pipelines. The operator is built with controller-runtime and handles all aspects of Pipeline lifecycle management including creation, updates, status tracking, and deletion.

## Getting Started

### Prerequisites
- go version v1.24.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/openfilter-pipelines-controller:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/openfilter-pipelines-controller:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/openfilter-pipelines-controller:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/openfilter-pipelines-controller/<tag or branch>/dist/install.yaml
```

### Using Helm Chart from GitHub Pages

The Helm chart is automatically published to GitHub Pages on each release. To install using Helm:

1. Add the Helm repository:

```sh
helm repo add openfilter-pipelines https://plainsightai.github.io/openfilter-pipelines-controller
helm repo update
```

2. Install the chart:

```sh
helm install openfilter-pipelines-controller openfilter-pipelines/openfilter-pipelines-controller \
  --namespace pipelines-system \
  --create-namespace
```

3. Install with Valkey enabled (optional):

```sh
helm install openfilter-pipelines-controller openfilter-pipelines/openfilter-pipelines-controller \
  --namespace pipelines-system \
  --create-namespace \
  --set valkey.enabled=true
```

4. To upgrade an existing installation:

```sh
helm repo update
helm upgrade openfilter-pipelines-controller openfilter-pipelines/openfilter-pipelines-controller \
  --namespace pipelines-system
```

For more configuration options, see the [chart values](charts/openfilter-pipelines-controller/values.yaml).

## Contributing

We welcome contributions to the openfilter-pipelines-controller project! Here's how you can contribute:

1. **Report Issues**: If you find bugs or have feature requests, please open an issue on the GitHub repository.

2. **Submit Pull Requests**:
   - Fork the repository and create a feature branch
   - Make your changes and ensure tests pass (`make test`)
   - Run linting checks (`make lint`) and fix any issues
   - Submit a pull request with a clear description of your changes

3. **Code Standards**:
   - Follow Go code conventions and best practices
   - Run `make fmt` to format code and `make vet` to check for issues
   - Ensure all tests pass before submitting a PR
   - Update API types with kubebuilder markers if modifying CRDs
   - Run `make manifests` and `make helm-update-crds` after API changes

4. **Development Setup**:
   - Install Go 1.25.1+ and Docker
   - Install Kind for e2e testing
   - Use `make test` for unit tests and `make test-e2e` for integration tests

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
