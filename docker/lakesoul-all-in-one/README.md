# Single-container LakeSoul

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

One container runs:

- PostgreSQL 18 for LakeSoul metadata (`127.0.0.1:5432`, database `lakesoul`);
- one `lakesoul-worker` (`127.0.0.1:50051`);
- `postgres-lakesoul` on the host at `127.0.0.1:5433`.

JWT authentication and RBAC are off by default. IVM is always installed and
new connections start with `lakesoul.ivm.enabled = on`. This is a local,
single-trusted-node shape; do not publish port 5433 on an untrusted network.

## Build

The image is built by the repository flake, so it reuses the Rust dependency
cache and includes the Nix closure of PostgreSQL and the binaries:

```bash
nix build .#all-in-one-image
./result | docker load
```

The image is `lakesoul-all-in-one:dev`.

## Run

```bash
docker compose -f docker/lakesoul-all-in-one/compose.yaml up -d
psql "postgresql://lakesoul:lakesoul@127.0.0.1:5433/default"
```

The compose file bind-mounts, relative to the directory where you run it:

| Host path | Container path | Purpose |
| --- | --- | --- |
| `${PWD}/warehouse` | `/warehouse` | table data (`file:///warehouse`) |
| `${PWD}/pgdata` | `/var/lib/postgresql/data` | metadata PostgreSQL |
| `${PWD}/scratch` | `/var/scratch` | spill and temp files |

`docker compose down` keeps the bind mounts. Delete the directories to start
from scratch.

The equivalent `docker run`:

```bash
docker run -d --name lakesoul-all-in-one \
  --restart unless-stopped \
  -p 127.0.0.1:5433:5433 \
  -e POSTGRES_PASSWORD=lakesoul \
  -e WORKER_MEMORY_LIMIT=4Gi \
  -v "$PWD/warehouse:/warehouse" \
  -v "$PWD/pgdata:/var/lib/postgresql/data" \
  -v "$PWD/scratch:/var/scratch" \
  lakesoul-all-in-one:dev
```

The first start runs `initdb` and applies `script/meta_init.sql`. Later starts
reuse the data directory; apply metadata migrations when upgrading from an
older LakeSoul release instead of deleting it.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `POSTGRES_USER` | `lakesoul` | metadata user |
| `POSTGRES_PASSWORD` | `lakesoul` | metadata password (also used by the coordinator) |
| `POSTGRES_DB` | `lakesoul` | metadata database |
| `LAKESOUL_WAREHOUSE` | `file:///warehouse` | table data prefix |
| `LAKESOUL_PG_PORT` | `5433` | PG wire port |
| `LAKESOUL_PG_IVM_DEFAULT` | `true` | new sessions start with IVM on |
| `LAKESOUL_PG_JWT_AUTH` | `false` | JWT authentication |
| `LAKESOUL_PG_RBAC` | `false` | domain enforcement (requires JWT) |
| `WORKER_THREADS` | `4` | worker Tokio threads |
| `WORKER_MEMORY_LIMIT` | `4Gi` | worker DataFusion execution pool |
| `WORKER_VORTEX_DECODE_BUDGET` | `1Gi` | worker Vortex decode budget |
| `COORDINATOR_THREADS` | `4` | coordinator Tokio threads |
| `COORDINATOR_MEMORY_LIMIT` | `2Gi` | coordinator DataFusion execution pool |
| `COORDINATOR_VORTEX_DECODE_BUDGET` | `512Mi` | coordinator Vortex decode budget |
| `LAKESOUL_DISTRIBUTED_TARGET_PARTITIONS` | `4` | lanes per distributed task |

The worker and coordinator are separate processes, but Docker only limits the
whole container. `WORKER_MEMORY_LIMIT` is a DataFusion execution budget, not a
cgroup limit. For hard per-worker isolation, run the worker as its own service
(compose split) or use the Kubernetes chart.

The container runs every process as `nobody` (65534) after the entrypoint has
chowned the bind mounts.

The container healthcheck runs `pg_isready` against the PG wire port. Query
execution still needs the worker; the entrypoint exits the container if the
PG, worker or coordinator process dies, and the restart policy starts it again.

## Verify

```sql
CREATE TABLE orders (id bigint PRIMARY KEY, amount numeric(20,4))
  WITH (hash_bucket_num = 4);
INSERT INTO orders VALUES (1, 42.0);
SELECT * FROM orders;

SET lakesoul.ivm.enabled = on;
SHOW lakesoul.ivm.enabled;
```

`SHOW lakesoul.ivm.enabled` is `on` on a new connection because the entrypoint
sets `LAKESOUL_PG_IVM_DEFAULT=true`.

Small queries may still run on the coordinator. The distributed planner keeps
scans below `LAKESOUL_DISTRIBUTED_BYTES_PER_PARTITION * target_partitions` local;
lower that value or use a larger table to force work onto the worker.

## Data safety

- `pgdata` and `warehouse` must be backed up together; metadata without table
  files, or table files without metadata, cannot be recovered.
- The image is a development/evaluation shape. It runs one PostgreSQL instance
  with no HA and one worker; production deployments should use the Kubernetes
  chart with an external metadata PostgreSQL and object storage.
