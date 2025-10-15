{{/*
Expand the name of the chart.
*/}}
{{- define "openfilter-pipelines-runner.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "openfilter-pipelines-runner.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "openfilter-pipelines-runner.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "openfilter-pipelines-runner.labels" -}}
helm.sh/chart: {{ include "openfilter-pipelines-runner.chart" . }}
{{ include "openfilter-pipelines-runner.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "openfilter-pipelines-runner.selectorLabels" -}}
app.kubernetes.io/name: {{ include "openfilter-pipelines-runner.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "openfilter-pipelines-runner.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "openfilter-pipelines-runner.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the pipeline executor service account to use
This service account is always created as it's required for pipeline execution
*/}}
{{- define "openfilter-pipelines-runner.pipelineExecutorServiceAccountName" -}}
{{- default "pipeline-exec" .Values.pipelineExecutor.serviceAccount.name }}
{{- end }}
