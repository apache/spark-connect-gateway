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
# Report RustSec advisories that a change introduces, leaving out ones that are
# already present on the revision it is based on.
#
#   dev/audit-new-advisories.sh BASE_LOCKFILE [HEAD_LOCKFILE]
#
# HEAD_LOCKFILE defaults to ./Cargo.lock. Both lockfiles are checked with
# `cargo audit`, which honours .cargo/audit.toml, against the same snapshot of
# the advisory database.
#
# Exit status: 0 if HEAD has no advisory that BASE lacks, 1 if it does, 2 on a
# usage error or if `cargo audit` could not produce a report.
#
# This is what CI runs on pull requests. Advisories already present on main are
# reported by the scheduled run instead, so a pull request is only held
# responsible for what it changes.
# USAGE-END

set -euo pipefail

usage() {
  sed -n '/^# USAGE-BEGIN$/,/^# USAGE-END$/{/USAGE-/d;s/^# \{0,1\}//;p;}' "$0"
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
esac
if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 2
fi
base="$1"
head="${2:-Cargo.lock}"
for f in "$base" "$head"; do
  if [[ ! -f "$f" ]]; then
    echo "error: no such file: $f" >&2
    exit 2
  fi
done

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# `cargo audit` exits 1 when it finds advisories, so its exit status cannot tell
# a finding from a failure (say, the advisory database could not be fetched).
# Anything that is not a JSON report counts as a failure.
audit() {
  local lockfile="$1" out="$2"
  shift 2
  cargo audit --json --file "$lockfile" "$@" >"$out" 2>"$tmp/stderr" || true
  if ! jq -e '.vulnerabilities.list' "$out" >/dev/null 2>&1; then
    echo "error: cargo audit did not produce a report for $lockfile:" >&2
    cat "$tmp/stderr" >&2
    exit 2
  fi
}

# One line per advisory and affected crate. Versions are left out on purpose: a
# change that moves a crate from one affected version to another has not
# introduced anything new.
keys() {
  jq -r '.vulnerabilities.list[] | "\(.advisory.id) \(.package.name)"' "$1" | sort -u
}

# The first run fetches the advisory database; the second reuses that copy, so
# an advisory published in between cannot show up as introduced by the change.
audit "$head" "$tmp/head.json"
audit "$base" "$tmp/base.json" --no-fetch
keys "$tmp/head.json" >"$tmp/head"
keys "$tmp/base.json" >"$tmp/base"

new="$(comm -13 "$tmp/base" "$tmp/head")"
existing="$(comm -12 "$tmp/base" "$tmp/head")"
resolved="$(comm -23 "$tmp/base" "$tmp/head")"

report() {
  local title="$1" body="$2"
  [[ -n "$body" ]] || return 0
  echo "$title"
  sed 's/^/  /' <<<"$body"
}
report "Resolved by this change:" "$resolved"
report "Already present on the base revision (not counted here):" "$existing"

if [[ -n "$new" ]]; then
  report "Introduced by this change:" "$new"
  exit 1
fi
echo "No advisories introduced by this change."
