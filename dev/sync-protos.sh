#!/usr/bin/env bash
#
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

# USAGE-BEGIN
# Compare the vendored Spark Connect protocol definitions in proto/ against
# apache/spark, or replace them with a given upstream revision.
#
# See proto/PROVENANCE.md for where these files came from and why the default
# revision below is what it is.
#
#   dev/sync-protos.sh                  # report drift against the default ref
#   dev/sync-protos.sh --ref v4.3.0     # report drift against another ref
#   dev/sync-protos.sh --sync           # overwrite proto/ from the ref
#
# Checking is the default on purpose. Overwriting these files changes the
# gateway's wire protocol, so it should be a deliberate act reviewed on its own
# -- not a side effect of running a script to see where things stand.
# USAGE-END

set -euo pipefail

# The upstream revision proto/PROVENANCE.md records the vendored files as being
# at. Bump this together with that file whenever the protos are re-synced, so the
# two never disagree about what proto/ contains.
UPSTREAM_REF="v4.2.0"

# Upstream location of the protocol definitions within apache/spark.
UPSTREAM_DIR="sql/connect/common/src/main/protobuf/spark/connect"

mode="check"
while [ $# -gt 0 ]; do
  case "$1" in
    --sync) mode="sync"; shift ;;
    --ref) UPSTREAM_REF="${2:?--ref needs a value}"; shift 2 ;;
    -h|--help)
      # Print the usage block above rather than keeping a second copy of it:
      # everything between the markers, with the comment prefix stripped.
      sed -n '/^# USAGE-BEGIN$/,/^# USAGE-END$/p' "$0" \
        | grep -v -e '^# USAGE-BEGIN$' -e '^# USAGE-END$' \
        | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

repo_root=$(cd "$(dirname "$0")/.." && pwd)
local_dir="$repo_root/proto/spark/connect"
base_url="https://raw.githubusercontent.com/apache/spark/${UPSTREAM_REF}/${UPSTREAM_DIR}"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "vendored: proto/spark/connect"
echo "upstream: apache/spark ${UPSTREAM_REF} ${UPSTREAM_DIR}"
echo

# Fetch the upstream file list rather than iterating over what we already have,
# so that a file added upstream shows up as drift instead of being invisible.
if ! curl -fsS -m 60 \
    "https://api.github.com/repos/apache/spark/contents/${UPSTREAM_DIR}?ref=${UPSTREAM_REF}" \
    > "$tmp/listing.json"; then
  echo "could not list the upstream directory -- is ${UPSTREAM_REF} a real ref?" >&2
  exit 1
fi

python3 -c '
import json, sys
with open(sys.argv[1]) as fh:
    entries = json.load(fh)
if not isinstance(entries, list):
    sys.exit("unexpected response from the GitHub contents API")
names = sorted(e["name"] for e in entries
               if e.get("type") == "file" and e["name"].endswith(".proto"))
if not names:
    sys.exit("no .proto files found upstream")
print("\n".join(names))
' "$tmp/listing.json" > "$tmp/upstream-names"

same=0
differ=0
missing_local=0
extra_local=0

while read -r name; do
  [ -n "$name" ] || continue
  if ! curl -fsS -m 60 -o "$tmp/$name" "$base_url/$name"; then
    echo "  FETCH-FAILED  $name" >&2
    exit 1
  fi
  if [ ! -f "$local_dir/$name" ]; then
    echo "  ADDED-UPSTREAM $name  (not vendored here)"
    missing_local=$((missing_local + 1))
    [ "$mode" = "sync" ] && cp "$tmp/$name" "$local_dir/$name"
    continue
  fi
  if cmp -s "$local_dir/$name" "$tmp/$name"; then
    same=$((same + 1))
  else
    added=$(diff "$local_dir/$name" "$tmp/$name" | grep -c '^>' || true)
    removed=$(diff "$local_dir/$name" "$tmp/$name" | grep -c '^<' || true)
    echo "  DIFFERS  $name  (+${added}/-${removed} lines vs upstream)"
    differ=$((differ + 1))
    [ "$mode" = "sync" ] && cp "$tmp/$name" "$local_dir/$name"
  fi
done < "$tmp/upstream-names"

# A file we vendor that upstream no longer has is drift too, in the other
# direction -- most likely a rename or removal we have not picked up.
for path in "$local_dir"/*.proto; do
  name=$(basename "$path")
  if ! grep -qxF "$name" "$tmp/upstream-names"; then
    echo "  NOT-UPSTREAM  $name  (vendored here, absent from ${UPSTREAM_REF})"
    extra_local=$((extra_local + 1))
  fi
done

echo
echo "identical: ${same}   differing: ${differ}   only-upstream: ${missing_local}   only-local: ${extra_local}"

if [ "$mode" = "sync" ]; then
  echo
  echo "proto/ now matches ${UPSTREAM_REF}. Next:"
  echo "  1. cargo build -p scg-genproto    # the bindings are generated, not committed"
  echo "  2. cargo test --workspace"
  echo "  3. update the revision in proto/PROVENANCE.md and UPSTREAM_REF above"
  exit 0
fi

# Drift is the normal state between syncs, so report it without failing; the
# whole point of the check mode is to be runnable at any time to see where the
# vendored copy stands.
if [ $((differ + missing_local + extra_local)) -gt 0 ]; then
  echo "proto/ has drifted from ${UPSTREAM_REF}; see proto/PROVENANCE.md."
  echo "Re-sync with: dev/sync-protos.sh --sync --ref ${UPSTREAM_REF}"
fi
