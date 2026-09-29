<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Provenance of the vendored Spark Connect protocol definitions

The eleven `.proto` files under `spark/connect/` are **copied verbatim from
[apache/spark](https://github.com/apache/spark)**. They are not written or
maintained here, and they are not generated: they are checked-in schema source.

| | |
|---|---|
| Upstream project | `apache/spark` |
| Upstream path | `sql/connect/common/src/main/protobuf/spark/connect/` |
| Baseline revision | `v4.2.0` (see *Which revision* below) |
| Files vendored | all 11 `.proto` files in that directory |
| License | Apache-2.0 — the same license as this project; each file keeps its ASF header |

The Rust bindings are generated from these files at build time by
`crates/genproto/build.rs` (via `tonic-prost-build`) into `target/`, so no
generated code is committed. `crates/genproto/build.rs` reads every `.proto` in
the directory and fails if it is empty.

## Which revision

The vendored copy predates the `v4.2.0` release. It matches `v4.2.0` exactly for
9 of the 11 files, and the files themselves refer to fields "deprecated since
Spark 4.2+", so it was taken from the Spark 4.2 development line — but from an
untagged commit, not from a release. No `v4.2.0-rcN` tag matches either.

`v4.2.0` is recorded as the baseline revision because it is the earliest
*released* tag the vendored copy is consistent with, which makes it a meaningful
thing to diff against. It is not a claim that these files were taken from that
tag.

Two files differ from `v4.2.0`, in both cases because upstream has content the
vendored copy does not:

| File | Missing relative to `v4.2.0` |
|---|---|
| `spark/connect/relations.proto` | the `NearestByJoin` message and its `Relation.rel_type` entry |
| `spark/connect/pipelines.proto` | the `AutoCdcFlowDetails` message and its `import "spark/connect/expressions.proto"` |

Both are pure additions upstream: nothing in the vendored files was edited
locally. In other words the vendored protos are slightly **behind** upstream, not
forked from it.

This does not currently affect the gateway. It forwards each
`SparkConnectService` request onward as a whole and only ever reads the
session and identity metadata — nothing in `crates/proxy` inspects a plan's
`rel_type` or walks the relation tree. So a request carrying one of these newer
messages is still routed and forwarded correctly.

What the gap does mean is that anything which needs to *inspect* one of those
newer messages — a routing rule keyed on plan shape, say — would have to re-sync
first.

## Checking and re-syncing

`dev/sync-protos.sh` compares the vendored files against upstream and reports
drift:

```bash
dev/sync-protos.sh                   # against the recorded baseline (v4.2.0)
dev/sync-protos.sh --ref v4.3.0      # against some other upstream ref
```

It lists the upstream directory rather than iterating over the local files, so a
file **added** upstream is reported as drift instead of being silently missed. A
file vendored here but absent upstream is reported too.

Re-syncing is a separate, deliberate step:

```bash
dev/sync-protos.sh --sync --ref v4.2.0
```

Checking is the default because overwriting these files changes the protocol the
gateway speaks. After a `--sync`:

1. `cargo build -p scg-genproto` — regenerate the bindings and confirm they compile.
2. `cargo test --workspace` — the proxy's routing and session handling assume the
   message shapes.
3. Update the **baseline revision** in the table above and `UPSTREAM_REF` in
   `dev/sync-protos.sh` so they keep agreeing with each other.

A re-sync is worth reviewing on its own rather than folding into an unrelated
change, since it moves the wire protocol.

## For a release

These files are third-party content redistributed in the source release. They
are Apache-2.0 licensed and retain their upstream ASF headers, so they need no
separate entry in `LICENSE`, but the source release's `NOTICE`-side paperwork
should account for them as sourced from `apache/spark`. That belongs with the
`LICENSE-binary` / `NOTICE-binary` dependency-census work, which is still
outstanding.
