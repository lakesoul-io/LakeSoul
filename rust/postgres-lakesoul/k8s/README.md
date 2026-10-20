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
  EndpointSlice refresh and exits reporting the reason otherwise (token/CA,
  RBAC, unreachable API). Zero ready workers is not a start-up failure: the
  coordinator serves, queries fail with `no eligible worker` until a worker
  turns Ready, and the next poll picks it up without a restart. Snapshots
  older than `LAKESOUL_DISCOVERY_MAX_STALENESS_SECONDS` fail newly
  distributed queries instead of silently planning against outdated workers.

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
   (`/app/postgres-lakesoul`, `/app/lakesoul-worker`). During development the
   manifests use one moving tag, `lakesoul-k8s:dev`, with
   `imagePullPolicy: Always`. Build it with the root `flake.nix` (crane +
   `dockerTools.streamLayeredImage`):

   ```sh
   nix build .#image && ./result | docker load
   ```

   `nix build .#lakesoul` builds only the two binaries. Rust is the
   `rust-toolchain.toml` channel as resolved by the `rust-overlay` revision
   in `flake.lock` (nixpkgs' rustc is too old for the workspace).
   Dependencies are a separate derivation, rebuilt only when manifests,
   `Cargo.lock`, `.cargo/config.toml` or the toolchain change; a source
   change recompiles the workspace crates only. The image has no distro
   base: it holds the binaries' Nix closure (glibc included), CA
   certificates and `/etc/passwd`, and runs as `nobody` (65534). The flake
   sees only git-tracked files.

   The tag does not identify a build: `--version` and the `build_info`
   metric report the 12-character git commit (`-dirty` for uncommitted
   changes).

   With `Always` the kubelet resolves the tag against the registry on every
   Pod start and downloads only the layers it lacks, so nodes must reach a
   registry: an image only copied onto a node (`docker load`, `kind load`)
   fails with `ErrImagePull`. The `images:` entry in `base/kustomization.yaml`
   points `lakesoul-k8s` at
   `swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/lakesoul-k8s`; edit its
   `newName` for another registry (`kubectl` bundles kustomize for `apply -k`
   but has no `kustomize edit`). Push after each build:

   ```sh
   docker tag lakesoul-k8s:dev swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/lakesoul-k8s:dev
   docker push swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/lakesoul-k8s:dev
   ```

   The SWR repository is private: both ServiceAccounts list the pull Secret
   `lakesoul-registry` in `imagePullSecrets` (created in step 3). Pushing a
   new `dev` leaves the Pod spec unchanged, so `kubectl apply` restarts
   nothing; after each push run
   `kubectl -n lakesoul rollout restart deployment/lakesoul-coordinator deployment/lakesoul-worker`.

   The build applies `.cargo/config.toml` (`-Ctarget-feature=+avx2`), so
   every node that runs the image needs AVX2 (`grep -m1 -o avx2 /proc/cpuinfo`).
2. `kubectl apply -k rust/postgres-lakesoul/k8s/base` creates the
   `lakesoul` namespace, the coordinator (1 replica) and worker (2
   replicas) Deployments, the two Services, the ConfigMap, and the
   coordinator's namespaced Role/RoleBinding for EndpointSlice reads.
3. Prerequisites to apply first (no automation on purpose):
   - PostgreSQL metadata database initialized (`script/meta_init.sql`),
     reachable from the cluster at the URL stored in the secret
     `lakesoul-secrets/pg-url`;
   - object store (S3 or RustFS) with the warehouse bucket initialized,
     reachable from coordinator and workers.

   Create the secrets once, out of this repository (never commit credentials),
   after `kubectl apply -f rust/postgres-lakesoul/k8s/base/namespace.yaml`:
   - `kubectl -n lakesoul create secret generic lakesoul-secrets --from-literal=...`
     with the keys `pg-url`, `pg-username`, `pg-password`, `warehouse`,
     `aws-endpoint`, `aws-bucket`, `aws-access-key-id`, `aws-secret-access-key`.
     Secrets are referenced through `secretKeyRef`, not inline in CLI args, so
     plain `kubectl describe` does not print credentials.
   - the image pull Secret, from SWR's long-term login command
     (`docker login -u <region>@<AK> -p <login key> swr...`; temporary login
     keys expire):

     ```sh
     kubectl -n lakesoul create secret docker-registry lakesoul-registry \
       --docker-server=swr.cn-southwest-2.myhuaweicloud.com \
       --docker-username='cn-southwest-2@<AK>' --docker-password='<login key>'
     ```

     The ServiceAccount admission controller copies `imagePullSecrets` into a
     Pod only at Pod creation, and the kubelet reads the Secret at each pull:
     Pods created before the Secret existed recover on their next pull retry;
     Pods created before the ServiceAccount listed it need
     `kubectl -n lakesoul rollout restart`.

## Observability (optional overlay)

`observability/` is the base plus Grafana 12.2.5, Prometheus v3.2.1 and Tempo
2.7.2, the versions and configuration of `docker/observability`. Apply it
instead of `base/`; the base alone keeps running without it.

| Component | What it does |
|---|---|
| `lakesoul-tempo` | OTLP ingest (4317 gRPC, 4318 HTTP), 24 h of traces on the `lakesoul-tempo-data` PVC (10Gi), span metrics / service graphs remote-written to Prometheus |
| `lakesoul-prometheus` | Scrapes every Pod in the namespace labelled `app.kubernetes.io/name: lakesoul` on its `metrics` port (labels `pod`, `role` = container name); receives Tempo's remote writes; 15 d / 8 GB on the `lakesoul-prometheus-data` PVC (10Gi) |
| `lakesoul-grafana` | Datasources Prometheus, Tempo and the cluster's existing Loki (`lakeinsight-loki.lakeinsight`, fed by `lakeinsight-promtail`), plus the *LakeSoul Overview* dashboard shared with `docker/observability` |
| coordinator and worker patch | `OTEL_EXPORTER_OTLP_ENDPOINT=http://lakesoul-tempo:4317`, `service.instance.id` = Pod name; both Deployments roll on apply |

1. Mirror the three upstream images into the SWR repository the
   `images:` entries in `observability/kustomization.yaml` point at (the
   ServiceAccounts reuse the `lakesoul-registry` pull Secret):

   ```sh
   for image in grafana/grafana:12.2.5 prom/prometheus:v3.2.1 grafana/tempo:2.7.2; do
     docker pull "$image"
     docker tag "$image" "swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/${image#*/}"
     docker push "swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/${image#*/}"
   done
   ```

2. Create the Grafana admin password (user `admin`; no anonymous access,
   since every Pod in the cluster can reach the Service):

   ```sh
   kubectl -n lakesoul create secret generic lakesoul-grafana \
     --from-literal=admin-password="$(openssl rand -base64 24)"
   ```

3. `kubectl apply -k rust/postgres-lakesoul/k8s/observability`. Like the base,
   this resets the worker Deployment to its manifest's 2 replicas.
4. `kubectl -n lakesoul port-forward svc/lakesoul-grafana 3000:3000`, then
   http://localhost:3000. The password:
   `kubectl -n lakesoul get secret lakesoul-grafana -o jsonpath='{.data.admin-password}' | base64 -d`.

Grafana keeps no state across restarts (`emptyDir`): datasources and the
dashboard are re-provisioned, dashboards made in the UI are lost. The
Traces Drilldown app downloads from grafana.com in the background on every
start; without egress Grafana logs the failure and runs without it.

In Loki the LakeSoul streams carry promtail's labels, so
`{container="lakesoul-worker"}` selects the workers and
`{namespace="lakesoul", pod="<pod>"}` one replica. The trace view's *Logs*
link maps `service.name` to `container` for the same reason. *Save & test*
on the Loki datasource reports a parse error: the cluster's Loki 2.6.1 cannot
parse the `vector(1)+vector(1)` health query Grafana 12 sends. Log queries,
the dashboard panel and the trace-to-logs link are unaffected.

## Network requirements

| Flow | Purpose |
|---|---|
| coordinator -> Kubernetes API server (443) | EndpointSlice listing (poll) |
| coordinator -> worker Pods (TCP 50051) | protocol probes and task dispatch, direct to Pod IP (not through the worker Service's ClusterIP) |
| worker <-> worker | stage output exchange (`grpc` channel) |
| coordinator, workers -> S3/RustFS | warehouse reads/writes |
| coordinator -> metadata PostgreSQL (5432) | catalog/DDL/commits |
| DNS | `lakesoul-worker.lakesoul.svc` etc. |
| coordinator, workers -> `lakesoul-tempo` (4317) | OTLP trace export (observability overlay) |
| `lakesoul-prometheus` -> coordinator (19090), workers (19091) | metrics scrape, direct to Pod IP; Pod discovery via the API server (443) |
| `lakesoul-tempo` -> `lakesoul-prometheus` (9090) | span metrics remote write |
| `lakesoul-grafana` -> Prometheus (9090), Tempo (3200), `lakeinsight-loki.lakeinsight` (3100) | dashboards and Explore |

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
only where needed. With the observability overlay, Tempo's OTLP ingest and
Prometheus' remote-write receiver are unauthenticated write paths; without a
NetworkPolicy any Pod in the cluster can reach them.

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

7. **Scale workers to 0** (`kubectl -n lakesoul scale deployment
   lakesoul-worker --replicas=0`): within one poll the snapshot empties and
   every query fails with `no eligible worker` (`LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL`
   does not apply: it covers planner failures only). A coordinator restarted
   meanwhile starts anyway and logs `starting without an eligible worker`.
   Scaling back up restores queries on the next poll, without a restart.

No automated acceptance run exists in this phase; step 6's assertion values
must be read from the coordinator's log output at the time you execute it.
