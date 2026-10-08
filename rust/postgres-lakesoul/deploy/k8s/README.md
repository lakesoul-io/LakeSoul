# LakeSoul coordinator + workers on Kubernetes (phase 1)

Plain YAML (kustomize), one image, two entry points selected by `pod command`.
This documents the **minimum** container/K8s integration only: EndpointSlice
discovery in the coordinator, shared across all PG sessions.

## What this phase does and does not provide

Included:

- coordinator (`postgres-lakesoul`) discovers the worker Service's
  EndpointSlices on a poll interval, keeping an in-memory snapshot; all PG
  connections of the process share exactly one resolver and one polling loop;
- workers (`lakesoul-worker`) are discovered by Service name via the
  `kubernetes.io/service-name=<service>` label, the `grpc` TCP port is chosen
  by name, TLS/token handling reads the mounted service account token from
  disk on every request;
- bounded start-up: the coordinator waits (max 30 s) for the first successful
  refresh that contains at least one protocol-compatible worker, and exits
  reporting the reason otherwise; snapshots older than
  `LAKESOUL_DISCOVERY_MAX_STALENESS_SECONDS` fail newly distributed queries
  instead of silently planning against outdated workers.

Deliberately **not** included (later phases):

- no gRPC Health / `/readyz` protocol: do not wire liveness/readiness probes
  that pretend otherwise;
- no HA coordinator (1 replica), no Helm chart, no Operator, no HPA;
- no graceful shutdown, no automatic retry, no task migration, no query
  checkpointing, no application-level memory admission (the container
  `memory` limit is a cgroup fact, the binaries do not yet read it);
- discovery does **not** recover in-flight tasks of a dead worker and does
  **not** migrate old queries; queries already running on a worker that then
  leaves fail at dispatch.

## Lifecycle

1. Images build from this repository; a single image carries both binaries
   (`/app/postgres-lakesoul`, `/app/lakesoul-worker`). The manifests use
   `lakesoul-k8s:local` with `imagePullPolicy: IfNotPresent`. Two equivalent
   builds:
   - Docker (no Nix needed):
     `docker build -f rust/postgres-lakesoul/deploy/Dockerfile -t lakesoul-k8s:local .`
   - Nix (root `flake.nix`, crane + `dockerTools.streamLayeredImage`):

     ```sh
     nix build .#image && ./result | docker load
     docker tag "lakesoul-k8s:$(nix eval --raw .#image.imageTag)" lakesoul-k8s:local
     ```

     `nix build .#lakesoul` builds only the two binaries. Rust is the
     `rust-toolchain.toml` channel as resolved by the `rust-overlay` revision
     in `flake.lock` (nixpkgs' rustc is too old for the workspace).
     Dependencies are a separate derivation, rebuilt only when manifests,
     `Cargo.lock`, `.cargo/config.toml` or the toolchain change; a source
     change recompiles the workspace crates only. The image has no
     distro base: it holds the binaries' Nix closure (glibc included),
     CA certificates and `/etc/passwd`, runs as `nobody` (65534), and is
     tagged with the 12-character git commit (`-dirty` for uncommitted
     changes) that `--version` also reports. The flake sees only git-tracked
     files.

   Both builds apply `.cargo/config.toml` (`-Ctarget-feature=+avx2`), so
   every node that runs the image needs AVX2 (`grep -m1 -o avx2 /proc/cpuinfo`).
2. `kubectl apply -k rust/postgres-lakesoul/deploy/k8s/base` creates the
   `lakesoul` namespace, the coordinator (1 replica) and worker (2
   replicas) Deployments, the two Services, the ConfigMap, and the
   coordinator's namespaced Role/RoleBinding for EndpointSlice reads.
3. Prerequisites to apply first (no automation on purpose):
   - PostgreSQL metadata database initialized (`script/meta_init.sql`),
     reachable from the cluster at the URL stored in the secret
     `lakesoul-secrets/pg-url`;
   - object store (S3 or RustFS) with the warehouse bucket initialized,
     reachable from coordinator and workers.

   Create the secret once, out of this repository (never commit credentials):
   `kubectl -n lakesoul create secret generic lakesoul-secrets --from-literal=...`
   with the keys `pg-url`, `pg-username`, `pg-password`, `warehouse`,
   `aws-endpoint`, `aws-access-key-id`, `aws-secret-access-key`.
   Secrets are referenced through `secretKeyRef`, not inline in CLI args, so
   plain `kubectl describe` does not print credentials.

## Network requirements

| Flow | Purpose |
|---|---|
| coordinator -> Kubernetes API server (443) | EndpointSlice listing (poll) |
| coordinator -> worker Pods (TCP 50051) | protocol probes and task dispatch, direct to Pod IP (not through the worker Service's ClusterIP) |
| worker <-> worker | stage output exchange (`grpc` channel) |
| coordinator, workers -> S3/RustFS | warehouse reads/writes |
| coordinator -> metadata PostgreSQL (5432) | catalog/DDL/commits |
| DNS | `lakesoul-worker.lakesoul.svc` etc. |

No LoadBalancer or Ingress is configured: the PG wire endpoint lacks
authentication-visible deployment hardening in this phase (only the
metadata-DB user check), so it must not be exposed beyond the cluster. If a
NetworkPolicy is introduced it MUST allow the flows above — the easy
"default-deny-all-then-maybe" recipe that forgets the API server or
object-store flows breaks discovery or warehouse IO; prefer
namespaced traffic rules verified against your CNI's capabilities
(NetworkPolicies are enforced by the CNI, not the API server). One that is
safe to start with: an egress policy on the coordinator worker allowing
443/TCP to the API server CIDR and DNS, plus same-namespace ingress/egress
only where needed.

## Configuration reference (env; priority: CLI > env > default)

Coordinator (`/app/postgres-lakesoul`):
`LAKESOUL_PG_BIND` (default `127.0.0.1`; set `0.0.0.0` in the YAML),
`LAKESOUL_PG_PORT` (5432), `LAKESOUL_PG_METRICS_ADDR`,
`LAKESOUL_PG_MAX_CONNECTIONS/QUERIES_PER_USER/RESULT_ROWS/RESULT_BYTES`,
`LAKESOUL_WAREHOUSE`, `AWS_ENDPOINT`, `AWS_BUCKET`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_S3_VIRTUAL_HOST_STYLE`, `LAKESOUL_WORKER_THREADS`,
`LAKESOUL_DISTRIBUTED_DISCOVERY` (`none|static|kubernetes`),
`LAKESOUL_DISTRIBUTED_WORKERS` (static list; conflicts with kubernetes mode),
`LAKESOUL_K8S_WORKER_SERVICE`, `LAKESOUL_K8S_NAMESPACE` (injected from the
Downward API),
`LAKESOUL_DISCOVERY_POLL_INTERVAL_SECONDS` (5; at least 1),
`LAKESOUL_DISCOVERY_MAX_STALENESS_SECONDS` (30; 1..86400),
`LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL` (bool, must be parsed
`true|false`; bare `--distributed-fallback-local` flag keeps working),
`LAKESOUL_DISTRIBUTED_BYTES_PER_PARTITION`.
Metadata DB variables (`LAKESOUL_PG_URL/USERNAME/PASSWORD`) are unchanged.

Worker (`/app/lakesoul-worker`): `LAKESOUL_WORKER_BIND` (0.0.0.0),
`LAKESOUL_WORKER_PORT` (50051), `LAKESOUL_WORKER_METRICS_ADDR`,
`LAKESOUL_WORKER_THREADS`, plus the same warehouse/S3 variables; both binaries
share the `CoreArgs` CLI (`--warehouse-prefix`, `--endpoint`, …).

## Verification / acceptance plan (runs against a real cluster; NOT yet
executed — the manifests and the failure paths are untested beyond `cargo`
compile/test)

Scenarios, each with concrete expected state:

1. **Scale 2 -> 3 workers** (`kubectl -n lakesoul scale deployment
   lakesoul-worker --replicas=3`): the next 5 s poll reports 3 ready slices;
   a small query planned before the scale-up still runs (local/eligible
   snapshot), a later scan-fanout query may dispatch tasks to the new worker.
2. **Pod rebuild / IP change** (`kubectl -n lakesoul rollout restart
   deployment lakesoul-worker`): old addresses disappear from the snapshot
   once their endpoints leave the slices; in-flight queries to old IPs fail
   at dispatch (no automatic retry in phase 1 — expected); new queries run
   against the new IPs.
3. **Protocol mismatch**: run one worker with an older build advertising
   `lakesoul-distributed/1`; it must be excluded from the snapshot (only
   `DISTRIBUTED_PROTOCOL_VERSION` = `lakesoul-distributed/2` eligible) and be
   visible in the coordinator logs as a candidate (not eligible).
4. **RBAC failure**: bind the Role only in a different namespace (or drop
   `get/list`): first sync fails, startup aborts with `403 Forbidden` in the
   error even though workers are fine; the coordinator must not serve
   queries in that state.
5. **API unreachable**: block egress to the API server after a successful
   sync: queries keep working against the last snapshot until
   `LAKESOUL_DISCOVERY_MAX_STALENESS_SECONDS` elapse, then fail with the
   staleness error naming the pending refresh failure.
6. **Real remote stage**: prepare the data with a writable LakeSoul engine
   (Spark/DataFrame client, or the lakesoul console) — the PG entry exposes
   a read-only projection: INSERT/DDL through the coordinator process are
   rejected with SQLSTATE `0A000` by design. Then `SELECT count(*)` (or the
   row set) through the coordinator and confirm the coordinator's worker
   metrics/logs show an actual remote task execution.

   The acceptance check is remote **execution**, not remote **routing**: a
   "worker availability" URL list in planner logs is printed before physical
   planning and does not prove any stage ran elsewhere. Assert instead on:
   - worker-side stage/task-execution evidence: `lakesoul-worker` logs (or
     process metrics on worker Pods) showing at least one stage executed
     during the query — grep the worker pod logs around the query timestamps;
   - and/or the coordinator's dispatch accounting: per-query successful
     task dispatch to `http://<worker-pod-ip>:50051` (StageTaskPlan/work
     exchange), visible in coordinator logs at dedicated `dispatch`/info
     level.

   `SELECT 1` and similarly trivial queries never leave the coordinator by
   design: eligible workers only make the distributed planner available;
   the planner still decides stage-by-stage. Force a scan-fanout (large
   warehouse table, or lower `LAKESOUL_DISTRIBUTED_BYTES_PER_PARTITION`)
   when the workload defaults to single-node execution.

No automated acceptance run exists in this phase; step 6's assertion values
must be read from the coordinator's log output at the time you execute it.
