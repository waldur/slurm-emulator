{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "slurm-emulator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name. Truncated at 63 chars for DNS-1123 compliance.
*/}}
{{- define "slurm-emulator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Chart name + version label.
*/}}
{{- define "slurm-emulator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "slurm-emulator.labels" -}}
helm.sh/chart: {{ include "slurm-emulator.chart" . }}
{{ include "slurm-emulator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels (stable across upgrades).
*/}}
{{- define "slurm-emulator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "slurm-emulator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Resolve the image tag, defaulting to .Chart.AppVersion when empty.
*/}}
{{- define "slurm-emulator.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{/*
Name of the Secret holding UI credentials and the slurmrestd JWT key — either
the caller's own or the chart-managed one.
*/}}
{{- define "slurm-emulator.secretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else -}}
{{- include "slurm-emulator.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
persistence.path with any trailing slash stripped, so joined file paths never
end up with a double slash.
*/}}
{{- define "slurm-emulator.persistence.dir" -}}
{{- $dir := trimSuffix "/" .Values.persistence.path -}}
{{- if eq $dir "" -}}/{{- else -}}{{ $dir }}{{- end -}}
{{- end -}}
