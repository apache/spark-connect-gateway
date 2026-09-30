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
| Revision | `v4.2.0` — the vendored files match it exactly |
| Files vendored | all 11 `.proto` files in that directory |
| License | Apache-2.0 — the same license as this project; each file keeps its ASF header |

The Rust bindings are generated from these files at build time by
`crates/genproto/build.rs` (via `tonic-prost-build`) into `target/`, so no
generated code is committed. `crates/genproto/build.rs` reads every `.proto` in
the directory and fails if it is empty.

## Which revision

The vendored files are byte-identical to `v4.2.0`, synced by
`dev/sync-protos.sh --sync --ref v4.2.0` (SPARK-59857). `dev/sync-protos.sh`
with no arguments confirms this and reports `identical: 11`.

`v4.2.0` was chosen because it is the newest *released* Spark tag. `v4.3.0` exists
only as release candidates, and syncing to an rc would pin the gateway to a
protocol that can still change before release.

### History

The original vendored copy — everything before SPARK-59857 — predated the
`v4.2.0` release. It matched `v4.2.0` for 9 of the 11 files and the files
referenced fields "deprecated since Spark 4.2+", but no release tag matched it,
including `v4.2.0-rc1` through `rc5`: it came from an untagged commit on the
Spark 4.2 development line. It was behind upstream rather than forked from it,
missing the `NearestByJoin` message in `relations.proto` and
`AutoCdcFlowDetails` in `pipelines.proto`; the sync added both. Nothing had been
edited locally, then or now.

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

## How protocol drift affects the gateway

Useful context when deciding how urgent a re-sync is: the gateway forwards each
`SparkConnectService` request onward whole and reads only the session and identity
metadata. Nothing in `crates/proxy` inspects a plan's `rel_type` or walks the
relation tree, and nothing in the workspace matches on those enums, so a request
carrying a message type the vendored protos do not know about is still routed and
forwarded correctly.

The one place that does look inside a message is `crates/proxy/src/config_filter.rs`,
which matches on `ConfigRequest.operation.op_type` to withhold the backend token —
unrelated to the relation and plan types.

So falling behind upstream degrades gracefully. It becomes a real problem only for
something that needs to *inspect* a newer message, such as a routing rule keyed on
plan shape.

## For a release

These files are third-party content redistributed in the source release. They
are Apache-2.0 licensed and retain their upstream ASF headers, so they need no
separate entry in `LICENSE`, but the source release's `NOTICE`-side paperwork
should account for them as sourced from `apache/spark` at the revision recorded
above. That belongs with the `LICENSE-binary` / `NOTICE-binary` dependency-census
work, which is still outstanding.
