{{/*
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
*/}}

{{- define "lakesoul-pg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "lakesoul-pg.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "lakesoul-pg.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "lakesoul-pg.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "lakesoul-pg.labels" -}}
helm.sh/chart: {{ include "lakesoul-pg.chart" . }}
app.kubernetes.io/name: {{ include "lakesoul-pg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "lakesoul-pg.selectorLabels" -}}
app.kubernetes.io/name: {{ include "lakesoul-pg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "lakesoul-pg.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | toString) }}
{{- end }}

{{- define "lakesoul-pg.coordinatorServiceAccountName" -}}
{{- default (printf "%s-coordinator" (include "lakesoul-pg.fullname" .)) .Values.serviceAccount.coordinatorName }}
{{- end }}

{{- define "lakesoul-pg.workerServiceAccountName" -}}
{{- default (printf "%s-worker" (include "lakesoul-pg.fullname" .)) .Values.serviceAccount.workerName }}
{{- end }}

{{- define "lakesoul-pg.runnerServiceAccountName" -}}
{{- default (printf "%s-runner" (include "lakesoul-pg.fullname" .)) .Values.serviceAccount.runnerName }}
{{- end }}

{{- define "lakesoul-pg.metadataSecretName" -}}
{{- if .Values.metadata.existingSecret }}
{{- .Values.metadata.existingSecret }}
{{- else if and .Values.metadata.postgresql.enabled .Values.metadata.postgresql.existingSecret }}
{{- .Values.metadata.postgresql.existingSecret }}
{{- else }}
{{- printf "%s-metadata" (include "lakesoul-pg.fullname" .) }}
{{- end }}
{{- end }}

{{- define "lakesoul-pg.warehouseSecretName" -}}
{{- default (printf "%s-warehouse" (include "lakesoul-pg.fullname" .)) .Values.warehouse.existingSecret }}
{{- end }}

{{- define "lakesoul-pg.workerServiceName" -}}
{{- default (printf "%s-worker" (include "lakesoul-pg.fullname" .)) .Values.distributed.workerService }}
{{- end }}

{{- define "lakesoul-pg.postgresqlServiceName" -}}
{{- printf "%s-postgresql" (include "lakesoul-pg.fullname" .) }}
{{- end }}

{{- define "lakesoul-pg.postgresqlDatabase" -}}
{{- default "lakesoul" .Values.metadata.postgresql.database }}
{{- end }}

{{- define "lakesoul-pg.postgresqlUsername" -}}
{{- default "lakesoul" .Values.metadata.postgresql.username }}
{{- end }}

{{- define "lakesoul-pg.metadataEnv" -}}
- name: LAKESOUL_PG_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.metadataSecretName" . }}
      key: pg-url
- name: LAKESOUL_PG_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.metadataSecretName" . }}
      key: pg-username
- name: LAKESOUL_PG_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.metadataSecretName" . }}
      key: pg-password
{{- end }}

{{- define "lakesoul-pg.warehouseEnv" -}}
{{- if eq .Values.warehouse.type "pvc" }}
- name: LAKESOUL_WAREHOUSE
  value: file:///warehouse
{{- else }}
- name: LAKESOUL_WAREHOUSE
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.warehouseSecretName" . }}
      key: warehouse
- name: AWS_ENDPOINT
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.warehouseSecretName" . }}
      key: aws-endpoint
- name: AWS_BUCKET
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.warehouseSecretName" . }}
      key: aws-bucket
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.warehouseSecretName" . }}
      key: aws-access-key-id
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.warehouseSecretName" . }}
      key: aws-secret-access-key
- name: AWS_S3_VIRTUAL_HOST_STYLE
  valueFrom:
    secretKeyRef:
      name: {{ include "lakesoul-pg.warehouseSecretName" . }}
      key: aws-s3-virtual-host-style
      optional: true
{{- end }}
{{- end }}

{{- define "lakesoul-pg.warehouseVolume" -}}
{{- if eq .Values.warehouse.type "pvc" }}
- name: warehouse
  persistentVolumeClaim:
    claimName: {{ default (printf "%s-warehouse" (include "lakesoul-pg.fullname" .)) .Values.warehouse.pvc.existingClaim }}
{{- end }}
{{- end }}

{{- define "lakesoul-pg.warehouseVolumeMount" -}}
{{- if eq .Values.warehouse.type "pvc" }}
- name: warehouse
  mountPath: /warehouse
{{- end }}
{{- end }}


