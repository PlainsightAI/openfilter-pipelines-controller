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

chart_lines = open(chart_path).read().splitlines(keepends=True)

# Locate the manager ClusterRole document by its kind+name pair: split the
# template on `---` lines and pick the document whose TOP-LEVEL kind is
# ClusterRole and whose name ends in `-manager-role`. Both checks matter, and
# the kind check must be unindented: the manager ClusterRoleBinding's roleRef
# block contains the indented pair `kind: ClusterRole` + `name: …-manager-role`,
# so a whitespace-stripped or plain substring match would splice into the
# binding if the documents were ever reordered. The rules block runs to the end
# of the document.
seps = [i for i, l in enumerate(chart_lines) if l.rstrip("\n") == "---"]
starts = [0] + [i + 1 for i in seps]
ends = seps + [len(chart_lines)]
target = None
for start, end in zip(starts, ends):
    doc = chart_lines[start:end]
    if any(l.rstrip("\n") == "kind: ClusterRole" for l in doc) and any(
        l.strip().startswith("name:") and l.rstrip("\n").endswith("-manager-role")
        for l in doc
    ):
        target = (start, end)
        break
if target is None:
    sys.exit("chart rbac.yaml has no manager-role ClusterRole")

start, end = target
rules_idx = next(
    (i for i in range(start, end) if chart_lines[i].rstrip("\n") == "rules:"), None
)
if rules_idx is None:
    sys.exit("manager-role ClusterRole has no rules block")

generated = (
    "# BEGIN manager-role rules — generated from config/rbac/role.yaml by\n"
    "# hack/update-helm-rbac.sh (make helm-update-rbac); do not edit by hand.\n"
    + rules
    + "# END manager-role rules\n"
)

open(chart_path, "w").write(
    "".join(chart_lines[: rules_idx + 1]) + generated + "".join(chart_lines[end:])
)
print("synced manager ClusterRole rules from config/rbac/role.yaml")
PYEOF

echo "OK — chart manager ClusterRole rules are in sync with config/rbac/role.yaml"
