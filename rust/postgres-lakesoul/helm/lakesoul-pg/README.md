# LakeSoul PostgreSQL Helm chart

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

This chart deploys the LakeSoul PostgreSQL wire coordinator, its distributed
workers, and (optionally) the standalone subscription runner.

The chart vendors `files/meta_init.sql` for the optional in-cluster
PostgreSQL/dev profile and the external init Job. Keep it in sync with
`script/meta_init.sql` when the metadata schema changes.

It is the Helm shape of `rust/postgres-lakesoul/k8s/base`; the kustomize
directory remains the minimal hand-applied example.

## Production shape

- external PostgreSQL 14+ with the metadata schema from
  `script/meta_init.sql` or `script/metadata_migrate.py`;
- S3 / RustFS warehouse;
- one coordinator and at least two workers;
- `subscriptions.mode=runner` so subscribers do not share the coordinator Pod.

Create the secrets first:

```bash
kubectl -n lakesoul create secret generic lakesoul-metadata \
  --from-literal=pg-url='jdbc:postgresql://metadata:5432/lakesoul?stringtype=unspecified' \
  --from-literal=pg-username=lakesoul \
  --from-literal=pg-password='...'

kubectl -n lakesoul create secret generic lakesoul-warehouse \
  --from-literal=warehouse='s3://bucket/warehouse' \
  --from-literal=aws-endpoint='https://s3.example.com' \
  --from-literal=aws-bucket='bucket' \
  --from-literal=aws-access-key-id='...' \
  --from-literal=aws-secret-access-key='...' \
  --from-literal=aws-s3-virtual-host-style='true'
```

Then:

```bash
helm install lakesoul rust/postgres-lakesoul/helm/lakesoul-pg \
  --namespace lakesoul --create-namespace \
  --set metadata.existingSecret=lakesoul-metadata \
  --set warehouse.existingSecret=lakesoul-warehouse \
  --set image.repository=<registry>/lakesoul-k8s \
  --set image.tag=<tag>
```

Connect:

```bash
kubectl -n lakesoul port-forward svc/lakesoul-lakesoul-pg-coordinator 5432:5432
psql "postgresql://postgres@127.0.0.1:5432/default"
```

## Single-node / kind profile

`values-dev.yaml` creates a one-replica metadata PostgreSQL StatefulSet, a local
warehouse PVC, one worker, and the subscription runner:

```bash
helm install lakesoul rust/postgres-lakesoul/helm/lakesoul-pg \
  --namespace lakesoul --create-namespace \
  -f rust/postgres-lakesoul/helm/lakesoul-pg/values-dev.yaml \
  --set image.repository=<registry>/lakesoul-k8s \
  --set image.tag=<tag>
```

This profile is for evaluation only: the database has no HA or backup, and the
warehouse PVC is single-node (`ReadWriteOnce`). For more than one node use S3 or
a ReadWriteMany storage class and set `worker.replicas` accordingly.

## Subscription modes

| `subscriptions.mode` | Behaviour |
| --- | --- |
| `runner` (default) | `CREATE/DROP/ALTER SUBSCRIPTION` writes metadata only; the `subscription-runner` Deployment owns children. `DROP` and `ALTER ... DISABLE` wait up to 30 s for the runner, then return `55006` and leave `dropping`/`disabling` state for retry. |
| `coordinator` | The coordinator starts children itself, as in the kustomize base. The coordinator must stay at one replica; the chart schema enforces that. |

The runner is single-replica. It uses `subscription.progress.owner` and
`lease_expires_at` to avoid two runners managing one subscription, but this is
not yet a full HA protocol: the source PostgreSQL replication slot is the
ultimate duplicate guard for PostgreSQL sources, and MySQL sources currently
rely on the single runner replica.

## Values worth checking

- `coordinator.replicas` is fixed at `1`.
- `worker.replicas`, `worker.resources`, and `worker.execution.memoryLimit`:
  the execution budget must stay below the container memory limit.
- `ivm.*`: rendered into the shared ConfigMap, so coordinator and workers get
  identical `--ivm-*` options.
- `metadata.postgresql.enabled`: development only. Production uses
  `metadata.existingSecret`.
- `warehouse.type`: `s3` in production, `pvc` only for single-node evaluation.
- `networkPolicy.enabled`: opt-in; fill the CIDRs for the API server, metadata
  PostgreSQL, object store and source PostgreSQL.
- `observability.serviceMonitor.enabled`: requires Prometheus Operator CRDs.

## Lint and test

```bash
helm lint rust/postgres-lakesoul/helm/lakesoul-pg
helm template lakesoul rust/postgres-lakesoul/helm/lakesoul-pg \
  --set metadata.url=jdbc:postgresql://metadata:5432/lakesoul \
  --set metadata.username=lakesoul --set metadata.password=secret \
  --set warehouse.s3.endpoint=https://s3.example.com \
  --set warehouse.s3.bucket=bucket \
  --set warehouse.s3.accessKeyId=key --set warehouse.s3.secretAccessKey=secret \
  >/tmp/lakesoul.yaml
helm test lakesoul -n lakesoul
```

`helm test` uses `postgres:18` and expects JWT authentication to be off.
