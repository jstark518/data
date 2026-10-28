{{- define "streaming-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streaming-platform.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "streaming-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* StorageClass resolution: per-component override, else the global one, else the cluster default. */}}
{{- define "streaming-platform.storageClass" -}}
{{- $local := .local | default "" -}}
{{- $global := .global | default "" -}}
{{- if $local }}{{ $local }}{{ else if $global }}{{ $global }}{{ end -}}
{{- end -}}

{{/* In-cluster Kafka bootstrap address, for NOTES and for anything that needs to reach it. */}}
{{- define "streaming-platform.kafkaBootstrap" -}}
{{ .Values.kafka.clusterName }}-kafka-bootstrap.{{ .Values.kafka.namespace }}.svc.cluster.local:9092
{{- end -}}

{{- define "streaming-platform.esService" -}}
elasticsearch.{{ .Values.elasticsearch.namespace }}.svc.cluster.local
{{- end -}}

{{- define "streaming-platform.esHosts" -}}
{{- if .Values.kibana.elasticsearch.hosts -}}
{{ .Values.kibana.elasticsearch.hosts }}
{{- else -}}
{{ if .Values.elasticsearch.security.enabled }}https{{ else }}http{{ end }}://elasticsearch:9200
{{- end -}}
{{- end -}}
