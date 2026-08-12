# Integration tests

Two flavours.

## In-process tests (`crates/proxy/tests/forwarding.rs`)

These run against in-process fake Spark Connect backends built with `tonic`.
They cover:

- Unary RPC forwarding (`Config`)
- Server-streaming forwarding emits all upstream messages (`ExecutePlan`)
- Session affinity stickiness across many calls
- Op-id reverse index — a `ReattachExecute` with a *different* `session_id`
  still routes to the backend that handled the original `ExecutePlan`

Run them with the rest of the test suite:

```bash
cargo test --workspace
```

## Real Spark Connect E2E (manual)

This validates the gateway against a real Spark Connect server using a real
PySpark client. It requires Docker.

The flow has been validated end-to-end with `apache/spark:4.0.0` and
`pyspark[connect]==4.0.0`. The non-obvious gotchas below are why the
commands look the way they do.

### Gotchas worth knowing up front

1. **Do not use `start-connect-server.sh` inside the container.** The
   shell script daemonizes the JVM and exits immediately, killing PID 1
   and taking the container down with it. Run `spark-submit` in the
   foreground instead — the command below already does this.
2. **Don't pass `--packages org.apache.spark:spark-connect_2.13:4.0.0`.**
   The `apache/spark:4.0.0` image already ships
   `/opt/spark/jars/spark-connect_2.13-4.0.0.jar`. Adding `--packages`
   makes Spark try to fetch from Maven Central, which fails behind
   restricted networks (and is wasteful in any case).
3. **Set `spark.connect.grpc.binding.host=0.0.0.0`.** The default binds
   to localhost inside the container, and Docker port forwarding then
   sees no listener on 0.0.0.0.

### 1. Start a Spark Connect server

```bash
docker run -d --name sc-server -p 15002:15002 apache/spark:4.0.0 \
  /opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --conf spark.connect.grpc.binding.port=15002 \
  --conf spark.connect.grpc.binding.host=0.0.0.0 \
  --name SparkConnectServer \
  local:///opt/spark/jars/spark-connect_2.13-4.0.0.jar
```

The server takes ~10–20 s to be ready. Wait until port 15002 is open:

```bash
until nc -z 127.0.0.1 15002; do sleep 2; done
docker logs sc-server | tail -5  # expect: "Spark Connect server started at: …:15002"
```

### 2. Configure and start the gateway

`config.yaml`:

```yaml
bind_addr: "127.0.0.1:15003"
backends:
  - "127.0.0.1:15002"
```

```bash
cargo run --release --bin gateway -- --config config.yaml
```

(The release build is fast to start and gives realistic latency. A debug
build also works.)

### 3. Run the PySpark client

In a separate shell, with PySpark + Spark Connect client installed
(use a virtualenv to avoid touching system Python):

```bash
python3 -m venv /tmp/sc-gw-venv
/tmp/sc-gw-venv/bin/pip install --upgrade pip
/tmp/sc-gw-venv/bin/pip install 'pyspark[connect]==4.0.0'
/tmp/sc-gw-venv/bin/python test/integration/client_smoke.py sc://localhost:15003
```

Expected output:

```
[OK] range(10).count() = 10
[OK] createDataFrame returned 3 rows with correct schema
[OK] 5 follow-up queries succeeded on the same session
```

The script asserts that:

- A trivial `spark.range(10).count()` returns `10`
- A 3-column DataFrame round-trips through the gateway with correct schema and rows
- Five follow-up queries on the same session succeed (exercising session affinity)

### 4. Cleanup

```bash
docker rm -f sc-server
rm -rf /tmp/sc-gw-venv
# stop the gateway with Ctrl-C
```

## Why two layers?

The in-process tests prove the gateway's gRPC forwarding logic is correct
without depending on a Spark distribution — they run in seconds and catch
regressions every commit. The real-Spark E2E test catches issues that only
show up against an actual Spark Connect server (Arrow batch encoding,
large message handling, HTTP/2 trailer propagation under real load,
PySpark client behaviour).

CI runs the in-process suite on every commit. The real-Spark E2E is run
on demand and as part of release validation.
