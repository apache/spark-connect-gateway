# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

{{/*
Expand the name of the chart.
*/}}
{{- define "scg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "scg.fullname" -}}
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
Chart name + version label value.
*/}}
{{- define "scg.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "scg.labels" -}}
helm.sh/chart: {{ include "scg.chart" . }}
{{ include "scg.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (used in pod templates and services).
*/}}
{{- define "scg.selectorLabels" -}}
app.kubernetes.io/name: {{ include "scg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector labels narrowed to the gateway pods (excludes the Redis pod).
*/}}
{{- define "scg.gateway.selectorLabels" -}}
{{ include "scg.selectorLabels" . }}
app.kubernetes.io/component: gateway
{{- end }}

{{/*
Full label set for gateway resources (includes helm.sh/chart etc.).
*/}}
{{- define "scg.gateway.labels" -}}
{{ include "scg.labels" . }}
app.kubernetes.io/component: gateway
{{- end }}

{{/*
Env-var name carrying one tenant's backend token. Takes the tenant
name as its argument. Sanitisation can collide (team-a vs team_a);
values.yaml documents the constraint.
*/}}
{{- define "scg.backendTokenEnvName" -}}
SCG_BACKEND_TOKEN_{{ regexReplaceAll "[^A-Za-z0-9]" . "_" | upper }}
{{- end }}

{{/*
Full label set for the bundled Redis resources.
*/}}
{{- define "scg.redis.labels" -}}
{{ include "scg.labels" . }}
app.kubernetes.io/component: redis
{{- end }}

{{/*
Selector labels narrowed to the bundled Redis pod.
*/}}
{{- define "scg.redis.selectorLabels" -}}
{{ include "scg.selectorLabels" . }}
app.kubernetes.io/component: redis
{{- end }}

{{/*
ServiceAccount name to use. Defaults to the release fullname; users can
override.
*/}}
{{- define "scg.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "scg.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Hostname of the bundled Redis Service (only used when redis.enabled).
We expose it as a helper so the ConfigMap and any external integration
points at one place.
*/}}
{{- define "scg.redis.host" -}}
{{- printf "%s-redis" (include "scg.fullname" .) }}
{{- end }}

{{/*
Effective Redis URL the gateway will dial. When redis.enabled is true
we synthesize redis://<svc>:6379; otherwise we fall back to
.Values.affinityStore.redis.url. Used by the ConfigMap.
*/}}
{{- define "scg.redis.url" -}}
{{- if .Values.redis.enabled }}
{{- printf "redis://%s:6379" (include "scg.redis.host" .) }}
{{- else }}
{{- required "affinityStore.redis.url is required when redis.enabled is false" .Values.affinityStore.redis.url }}
{{- end }}
{{- end }}
