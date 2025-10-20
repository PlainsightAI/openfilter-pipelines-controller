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

# This script updates the CRDs in the Helm chart from the config/crd/bases directory
# It should be run whenever CRDs are regenerated via 'make manifests'

SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CRD_SOURCE_DIR="${SCRIPT_ROOT}/config/crd/bases"
CHART_CRD_DIR="${SCRIPT_ROOT}/charts/openfilter-pipelines-controller/crds"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

function log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

function log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

function main() {
    log_info "Starting CRD sync from config/crd/bases to Helm chart..."

    # Check if source directory exists
    if [ ! -d "${CRD_SOURCE_DIR}" ]; then
        log_error "Source directory does not exist: ${CRD_SOURCE_DIR}"
        log_error "Please run 'make manifests' first to generate CRDs"
        exit 1
    fi

    # Count CRD files in source
    CRD_COUNT=$(find "${CRD_SOURCE_DIR}" -name "*.yaml" -type f | wc -l | tr -d ' ')
    if [ "${CRD_COUNT}" -eq 0 ]; then
        log_error "No CRD files found in ${CRD_SOURCE_DIR}"
        log_error "Please run 'make manifests' first to generate CRDs"
        exit 1
    fi

    log_info "Found ${CRD_COUNT} CRD file(s) in source directory"

    # Create chart CRD directory if it doesn't exist
    if [ ! -d "${CHART_CRD_DIR}" ]; then
        log_info "Creating Helm chart CRD directory: ${CHART_CRD_DIR}"
        mkdir -p "${CHART_CRD_DIR}"
    fi

    # Check for existing CRDs in chart
    if [ -d "${CHART_CRD_DIR}" ] && [ "$(ls -A ${CHART_CRD_DIR})" ]; then
        log_warn "Existing CRDs found in chart directory, they will be replaced"
        rm -f "${CHART_CRD_DIR}"/*.yaml
    fi

    # Copy CRDs from source to chart
    log_info "Copying CRDs from ${CRD_SOURCE_DIR} to ${CHART_CRD_DIR}..."
    cp "${CRD_SOURCE_DIR}"/*.yaml "${CHART_CRD_DIR}/"

    # Verify copy was successful
    COPIED_COUNT=$(find "${CHART_CRD_DIR}" -name "*.yaml" -type f | wc -l | tr -d ' ')
    if [ "${COPIED_COUNT}" -ne "${CRD_COUNT}" ]; then
        log_error "Copy verification failed: expected ${CRD_COUNT} files, found ${COPIED_COUNT}"
        exit 1
    fi

    # List copied files
    log_info "Successfully copied the following CRD files:"
    find "${CHART_CRD_DIR}" -name "*.yaml" -type f -exec basename {} \; | sort | while read -r file; do
        echo "  - ${file}"
    done

    # Check if git is available and show status
    if command -v git &> /dev/null; then
        if git rev-parse --git-dir > /dev/null 2>&1; then
            log_info ""
            log_info "Git status of changed files:"
            git status --short "${CHART_CRD_DIR}" || true
        fi
    fi

    log_info ""
    log_info "✓ CRD sync completed successfully!"
    log_info ""
    log_info "Next steps:"
    echo "  1. Review the changes: git diff ${CHART_CRD_DIR}"
    echo "  2. Test the chart: helm template test charts/openfilter-pipelines-controller"
    echo "  3. Lint the chart: cd charts/openfilter-pipelines-controller && helm lint ."
    echo "  4. Commit the changes: git add ${CHART_CRD_DIR} && git commit"
}

main "$@"
