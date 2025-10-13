# Pipeline Execution RBAC

This document describes the RBAC (Role-Based Access Control) resources required for pipeline execution Jobs.

## Overview

When a PipelineRun is created, the controller spawns a Kubernetes Job whose pods execute the pipeline filters. Each pod includes a "claimer" init container that:

1. Claims a work message from the Valkey stream
2. Downloads the input file from S3/MinIO
3. **Patches the pod's annotations** with queue metadata (`queue.valkey.mid`, `queue.file`, `queue.attempts`)

The third step requires Kubernetes RBAC permissions to patch pod resources.

## Resources

### ServiceAccount: `pipeline-exec`

**File**: `pipeline_exec_service_account.yaml`

This ServiceAccount is used by Job pods for pipeline execution. It is referenced in the Job's `spec.template.spec.serviceAccountName` field.

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: pipeline-exec
  namespace: system
```

### Role: `pipeline-exec-role`

**File**: `pipeline_exec_role.yaml`

This Role grants the minimum permissions required for the claimer init container to function:

- `get` on pods - to read pod information
- `patch` on pods - to update pod annotations with queue metadata

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: pipeline-exec-role
  namespace: system
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "patch"]
```

### RoleBinding: `pipeline-exec-rolebinding`

**File**: `pipeline_exec_role_binding.yaml`

This RoleBinding links the `pipeline-exec` ServiceAccount to the `pipeline-exec-role`.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: pipeline-exec-rolebinding
  namespace: system
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: pipeline-exec-role
subjects:
  - kind: ServiceAccount
    name: pipeline-exec
    namespace: system
```

## Usage

### In Controller Code

When the controller creates a Job for a PipelineRun, it should set:

```go
job := &batchv1.Job{
    Spec: batchv1.JobSpec{
        Template: corev1.PodTemplateSpec{
            Spec: corev1.PodSpec{
                ServiceAccountName: "pipeline-exec",
                // ... rest of pod spec
            },
        },
    },
}
```

### In Claimer Init Container

The claimer reads the pod name and namespace from environment variables and uses the in-cluster Kubernetes client to patch its own pod:

```go
// Environment variables set by Kubernetes
POD_NAME      = metadata.name
POD_NAMESPACE = metadata.namespace

// Patch operation
PATCH /api/v1/namespaces/{POD_NAMESPACE}/pods/{POD_NAME}
{
  "metadata": {
    "annotations": {
      "queue.valkey.mid": "<message-id>",
      "queue.file": "<file-path>",
      "queue.attempts": "<attempt-count>"
    }
  }
}
```

## Security Considerations

1. **Namespace-scoped**: The Role is namespace-scoped, limiting the blast radius if compromised
2. **Minimal permissions**: Only `get` and `patch` on pods - no delete, create, or list permissions
3. **Self-patching only**: While technically the pod could patch any pod in the namespace, the claimer implementation only patches its own pod
4. **No privileged access**: The ServiceAccount has no elevated privileges or access to secrets

## Installation

These RBAC resources are automatically installed when you deploy the controller:

```bash
# Install CRDs and RBAC
make install

# Or deploy the full controller
make deploy IMG=<your-registry>/openfilter-pipelines-runner:tag
```

The resources will be created in the controller's namespace (typically `openfilter-pipelines-runner-system`).

## Troubleshooting

### Error: "pods is forbidden: User cannot patch resource"

This indicates the ServiceAccount doesn't have patch permissions. Verify:

1. The Job's `serviceAccountName` is set to `pipeline-exec`
2. The RoleBinding exists and references the correct ServiceAccount
3. The Role includes `patch` verb for pods

```bash
# Check ServiceAccount
kubectl get sa pipeline-exec -n openfilter-pipelines-runner-system

# Check Role
kubectl get role pipeline-exec-role -n openfilter-pipelines-runner-system -o yaml

# Check RoleBinding
kubectl get rolebinding pipeline-exec-rolebinding -n openfilter-pipelines-runner-system -o yaml
```

### Error: "failed to create in-cluster config"

The claimer must run inside a Kubernetes pod. It cannot run locally outside a cluster.

## References

- [Kubernetes RBAC Documentation](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)
- [Service Accounts](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/)
- Design Document: Section 5.2 "Init claimer behavior"
- Design Document: Section 9 "Security & Permissions"
