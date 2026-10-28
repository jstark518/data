# streaming-platform

A Kafka → Flink → Elasticsearch substrate you can stand up in one command, then bring your own
jobs to.

| Component | What you get |
|---|---|
| **Kafka** | Strimzi cluster in KRaft mode (no ZooKeeper), 3 dual-role nodes, JMX→Prometheus metrics, consumer-lag exporter, declarative topics |
| **Flink** | A long-running **session** cluster with 2 JobManagers, HA on object storage, Prometheus metrics, optional web UI ingress |
| **Elasticsearch** | Single node, block storage, sized for modest data |
| **Kibana** | Pinned to the ES version, optional ingress |

Each is independently toggleable — take only what you need.

This chart deploys the *platform*. It deliberately contains no stream-processing jobs: you submit
those to the session cluster as `FlinkSessionJob` resources or with `flink run`.

---

## Prerequisites

The chart creates **custom resources**. The operators that reconcile them are **not** installed
here, because they are cluster-wide singletons you probably want to manage separately. Without
them the Kafka and Flink objects will be rejected by the API server.

| Needed | Why | Install |
|---|---|---|
| **Strimzi operator** (≥1.0, `kafka.strimzi.io/v1`) | Reconciles `Kafka` / `KafkaNodePool` / `KafkaTopic` | `helm install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator -n kafka --create-namespace` |
| **Flink Kubernetes operator** ≥1.15 | Reconciles `FlinkDeployment` | [Apache Flink operator docs](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) — also needs cert-manager |
| **Block StorageClass** (RWO) | Kafka and ES both want real block semantics — **not** CephFS/NFS | cluster-specific |
| **An S3-compatible bucket** | Flink HA + checkpoints. Only if `flink.highAvailability.enabled` | MinIO, Ceph RGW, AWS S3 |
| Ingress controller + cert-manager | Only if you enable the ingresses | cluster-specific |

> ⚠️ **Strimzi ≥1.0 requires node pools.** Older `v1beta2` / ZooKeeper-era manifests will not
> apply against it, and vice versa — this chart targets the newer API only.

---

## Install

```bash
helm install streaming ./streaming-platform \
  --set global.storageClass=YOUR-BLOCK-SC
```

Flink HA needs an S3 credentials Secret in the Flink namespace first. Without it the
JobManagers cannot write HA state and will crash-loop:

```bash
kubectl create namespace flink
kubectl -n flink create secret generic flink-s3 \
  --from-literal=access-key=... --from-literal=secret-key=...
```

Don't want object storage yet? Turn HA off and run a single JobManager:

```bash
helm install streaming ./streaming-platform \
  --set global.storageClass=YOUR-BLOCK-SC \
  --set flink.highAvailability.enabled=false \
  --set flink.jobManagerReplicas=1 \
  --set flink.s3.enabled=false \
  --set flink.state.checkpointsDir=file:///tmp/checkpoints \
  --set flink.state.savepointsDir=file:///tmp/savepoints
```

A worked, real-world configuration is in [`examples/homelab.yaml`](examples/homelab.yaml).

---

## Before you expose this

**The defaults are a trusted-LAN posture, not a production one.** They are honest about what they
are, but copy them deliberately rather than by accident:

- **Elasticsearch has security disabled** — plain HTTP, no auth. Anyone who can reach `:9200` can
  read, write and **delete** every index. Set `elasticsearch.security.enabled=true` and give
  Kibana credentials via `kibana.elasticsearch.existingSecret`.
- **The Flink REST endpoint accepts job submissions.** Exposing `flink.ingress` unauthenticated
  lets anyone who can reach it run arbitrary code on your cluster. Put an authenticating proxy in
  front, or leave the ingress off and use `kubectl port-forward`.
- **The external Kafka listener defaults to PLAINTEXT with no auth** (and is disabled by default
  for exactly that reason). For anything beyond a trusted segment, set `tls: true` and add
  authentication:

  ```yaml
  kafka:
    listeners:
      external:
        enabled: true
        tls: true
        extraListenerConfig:
          authentication:
            type: scram-sha-512
  ```

  then create a `KafkaUser` with ACLs for your producer.

---

## Things that will bite you

**Replication factor vs. replica count.** The defaults assume 3 brokers (`RF=3`,
`min.insync.replicas=2`). If you set `kafka.replicas: 1` you **must** drop the replication
settings to 1, or every topic creation fails:

```yaml
kafka:
  replicas: 1
  config:
    offsetsTopicReplicationFactor: 1
    transactionStateLogReplicationFactor: 1
    transactionStateLogMinIsr: 1
    defaultReplicationFactor: 1
    minInsyncReplicas: 1
```

**Two JobManagers need real object storage.** `flink.jobManagerReplicas: 2` requires
`highAvailability.enabled` *and* a `storageDir` on S3-style concurrent object storage. On a POSIX
volume the JobManagers race on Flink's JobResultStore.

**The S3 plugin jar name is version-specific.** `flink.s3.builtInPlugin` must name a jar that
exists in `/opt/flink/opt/` in your image. Change `flink.image` and you probably have to change
this too.

**Kibana must match Elasticsearch exactly.** Not "roughly" — it refuses to start otherwise.

**Renaming a Kafka listener reconfigures the brokers**, and the name appears in the CR status,
which is how you read the assigned external address. If you have anything scripted against a
particular name, set `kafka.listeners.external.name` to keep it.

**`metadataVersion` is a one-way upgrade.** Bump it only after every broker is running the new
`version`.

---

## Uninstalling keeps your data

Namespaces carry `helm.sh/resource-policy: keep`, and Kafka PVCs default to
`deleteClaim: false`. So `helm uninstall` leaves your namespaces, topics' data and ES volumes
behind — deleting a namespace takes everything in it, which is far too blunt an instrument for an
uninstall. Remove them deliberately when you actually mean to:

```bash
kubectl delete namespace kafka flink elastic   # destroys all data
```

---

## Verifying

```bash
kubectl -n kafka wait kafka/kafka --for=condition=Ready --timeout=600s
kubectl -n flink wait flinkdeployment/session \
  --for=jsonpath='{.status.jobManagerDeploymentStatus}'=READY --timeout=300s
kubectl -n elastic rollout status statefulset/elasticsearch --timeout=300s
```

Round-trip a message:

```bash
kubectl -n kafka run kcat -it --rm --restart=Never \
  --image=quay.io/strimzi/kafka:latest-kafka-4.2.0 -- \
  bin/kafka-console-producer.sh --bootstrap-server kafka-kafka-bootstrap:9092 --topic smoke-test
```

Submit a job to the session cluster:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkSessionJob
metadata:
  name: my-job
  namespace: flink
spec:
  deploymentName: session
  job:
    jarURI: https://example.com/my-job.jar
    parallelism: 2
```

---

## If you write stateful jobs on this

One hard-won rule, because the failure is silent and expensive:

> **Pair any topology change with one `stateless` deploy.**

Flink derives operator hashes from the job graph, so adding, removing or reordering an operator
means the previous checkpoint cannot be mapped onto the new program. With
`allowNonRestoredState=false` (the default) the job then fails to start and crash-loops — and the
symptom is simply that *writes stop dead at the instant of rollout*, while the deployment itself
reports success. Set explicit `uid()` on every operator from day one; that is what keeps hashes
stable across ordinary code changes.

---

## Values

See [`values.yaml`](values.yaml) — every key is commented, including why the non-obvious defaults
are what they are.
