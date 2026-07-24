#!/usr/bin/env bash

# Copyright 2025.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

# Syncs the Helm chart's manager ClusterRole rules from the generated
# config/rbac/role.yaml (the kubebuilder-marker output), the same way
# update-helm-crds.sh syncs CRDs. Run after 'make manifests' whenever RBAC
# markers change. Motivation: the chart role was hand-maintained and drifted
# from the generated role three times in one week (events, services, pods
# delete — see PLAT-1352), each a silent Forbidden on kustomize installs.

SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ROLE_SOURCE="${SCRIPT_ROOT}/config/rbac/role.yaml"
CHART_RBAC="${SCRIPT_ROOT}/deployment/openfilter-pipelines-controller/templates/rbac.yaml"

python3 - "$ROLE_SOURCE" "$CHART_RBAC" << 'PYEOF'
import sys

role_path, chart_path = sys.argv[1], sys.argv[2]

# Extract the rules list from the generated role: everything after the
# top-level `rules:` line (role.yaml contains a single ClusterRole).
role_lines = open(role_path).read().splitlines()
try:
    start = role_lines.index("rules:") + 1
except ValueError:
    sys.exit("config/rbac/role.yaml has no top-level 'rules:' — regenerate with 'make manifests'")
rules = "\n".join(role_lines[start:]).rstrip() + "\n"

chart = open(chart_path).read()
anchor = '-manager-role'

# Locate the manager ClusterRole's rules block: from the first `rules:` line
# after the manager-role name anchor, up to (excluding) the next `---`.
name_idx = chart.find(anchor)
if name_idx == -1:
    sys.exit("chart rbac.yaml has no manager-role ClusterRole")
rules_idx = chart.index("\nrules:\n", name_idx) + len("\nrules:\n")
end_idx = chart.index("\n---", rules_idx) + 1

generated = (
    "# BEGIN manager-role rules — generated from config/rbac/role.yaml by\n"
    "# hack/update-helm-rbac.sh (make helm-update-rbac); do not edit by hand.\n"
    + rules
    + "# END manager-role rules\n"
)

open(chart_path, "w").write(chart[:rules_idx] + generated + chart[end_idx:])
print("synced manager ClusterRole rules from config/rbac/role.yaml")
PYEOF

echo "OK — chart manager ClusterRole rules are in sync with config/rbac/role.yaml"
