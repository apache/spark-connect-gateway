#!/usr/bin/env python3
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

"""Check LICENSE-binary and licenses-binary/ against the gateway binary's crates.

The container image ships the gateway binary, which statically links every
Rust crate it depends on. LICENSE-binary is LICENSE followed by an appendix that
lists those crates by license, and licenses-binary/<crate>/ holds the license
files of each crate that is not used under Apache-2.0. The image installs them
as /LICENSE and /licenses/.

This script takes the crate list from `cargo tree` for the Linux x86_64 target
(the image's platform) and checks that:

  - the appendix of LICENSE-binary matches the crate list exactly, and
  - every crate not used under Apache-2.0 has a licenses-binary/<crate>/
    directory whose files are byte-identical to that crate's own license files.

Run it before each release, and after any change to Cargo.lock that adds or
removes a crate. With --fix it rewrites the appendix and copies the license
files from the cargo registry sources; review what it copied before committing.

A license expression that is not in NON_APACHE and does not offer Apache-2.0
fails the check. Decide how that crate is used, confirm the license is
Category A (https://www.apache.org/legal/resolved.html), and add it to
NON_APACHE and SECTIONS.

Usage: dev/check-license-binary.py [--fix]
"""

import json
import os
import re
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET = "x86_64-unknown-linux-gnu"
LICENSE = os.path.join(ROOT, "LICENSE")
LICENSE_BINARY = os.path.join(ROOT, "LICENSE-binary")
NOTICE = os.path.join(ROOT, "NOTICE")
NOTICE_BINARY = os.path.join(ROOT, "NOTICE-binary")
LICENSES_DIR = os.path.join(ROOT, "licenses-binary")

# The appendix sections, in order: (license, heading). The license is the one
# under which the crates in that section are used.
SECTIONS = [
    ("Apache-2.0", "Apache License, Version 2.0"),
    ("MIT", "MIT License"),
    ("Unicode-3.0", "Unicode License v3"),
    ("BSD-3-Clause", "BSD 3-Clause License"),
    ("MIT AND BSD-3-Clause", "MIT License and BSD 3-Clause License"),
    ("ISC", "ISC License"),
    ("Apache-2.0 AND ISC", "Apache License, Version 2.0 and ISC License"),
    ("Zlib", "zlib License"),
    ("CDLA-Permissive-2.0", "Community Data License Agreement - Permissive, Version 2.0"),
    ("BSL-1.0", "Boost Software License 1.0"),
]

# License expressions that do not offer Apache-2.0, mapped to the license the
# crate is used under (a key of SECTIONS).
NON_APACHE = {
    "MIT": "MIT",
    "Unlicense OR MIT": "MIT",
    "Unicode-3.0": "Unicode-3.0",
    "BSD-3-Clause": "BSD-3-Clause",
    "MIT AND BSD-3-Clause": "MIT AND BSD-3-Clause",
    "ISC": "ISC",
    "Apache-2.0 AND ISC": "Apache-2.0 AND ISC",
    "Zlib": "Zlib",
    "CDLA-Permissive-2.0": "CDLA-Permissive-2.0",
    "BSL-1.0": "BSL-1.0",
}

# Crates whose published package has no license file. The value is a crate from
# the same repository and license whose license file is used instead.
LICENSE_FROM = {
    "tonic-prost": "tonic",
}

# Directories in licenses-binary/ for code that cargo tree does not list: the
# Rust standard library's components that are not used under Apache-2.0 (see
# STD_PARAGRAPH). Their files come from the crate version that the toolchain in
# rust-toolchain.toml builds the standard library with, so this script cannot
# compare them; update them by hand when the toolchain changes.
STD_DIRS = {"compiler_builtins"}

LICENSE_FILE = re.compile(r"^(LICEN[CS]E|COPYING)", re.IGNORECASE)

RULE = "-" * 80

HEADER = """\

================================================================================

APACHE SPARK CONNECT GATEWAY SUBCOMPONENTS

The container image of Apache Spark Connect Gateway contains the gateway
binary, which statically links the third-party Rust crates listed below
(https://crates.io/crates/<crate>). Each crate is listed under the license it is
used under; where a crate offers a choice that includes the Apache License,
Version 2.0, that is the license used. The license files of each crate that is
not used under the Apache License, Version 2.0 alone are in
licenses/<crate>/.

The gateway binary also statically links the Rust standard library (core,
alloc, std, and the crates they depend on). It is licensed under the Apache
License, Version 2.0 or the MIT License, and is used under the Apache License,
Version 2.0, except for these parts:

    compiler_builtins  MIT AND Apache-2.0 WITH LLVM-exception
                       (licenses/compiler_builtins/)
    memchr             MIT, listed below

The container image is based on gcr.io/distroless/cc-debian12. The Debian
packages in that base image are not part of this list; their copyright and
license information is in /usr/share/doc/<package>/copyright in the image.

This list is generated by dev/check-license-binary.py.
"""


def run(args):
    return subprocess.run(
        args, cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout


def chosen_license(expr):
    if expr in NON_APACHE:
        return NON_APACHE[expr]
    if " AND " not in expr and " WITH " not in expr:
        alternatives = [a.strip() for a in re.split(r" OR |/", expr)]
        if "Apache-2.0" in alternatives:
            return "Apache-2.0"
    return None


def linked_crates():
    """Return {name: {version: license expression}} for the third-party crates
    linked into the gateway binary."""
    out = run([
        "cargo", "tree", "--locked", "-p", "gateway",
        "-e", "normal,no-proc-macro", "--target", TARGET,
        "--prefix", "none", "-f", "{p}|{l}",
    ])
    crates = {}
    for line in out.splitlines():
        package, _, expr = line.partition("|")
        if "(/" in package:  # A workspace crate, given by its local path.
            continue
        # cargo tree marks a crate it has already printed with " (*)".
        expr = expr.strip().removesuffix(" (*)")
        name, version = package.split()[:2]
        crates.setdefault(name, {})[version.lstrip("v")] = expr
    return crates


def source_dirs():
    """Return {(name, version): source directory} for registry packages."""
    meta = json.loads(run([
        "cargo", "metadata", "--locked", "--format-version", "1",
        "--filter-platform", TARGET,
    ]))
    return {
        (p["name"], p["version"]): os.path.dirname(p["manifest_path"])
        for p in meta["packages"]
        if p["source"]
    }


def license_files(directory):
    """The license files of a crate that apply to the license it is used under.

    UNLICENSE and the Apache-2.0 texts are left out: no crate in NON_APACHE is
    used under the Unlicense, and LICENSE-binary itself is the Apache-2.0 text.
    A COPYING file next to other license files is a pointer to them.
    """
    names = sorted(
        n for n in os.listdir(directory)
        if LICENSE_FILE.match(n)
        and not n.upper().startswith("UNLICENSE")
        and "APACHE" not in n.upper()
    )
    if len(names) > 1:
        names = [n for n in names if not n.upper().startswith("COPYING")]
    return names


def appendix(crates, errors):
    by_license = {key: [] for key, _ in SECTIONS}
    for name in sorted(crates):
        chosen = set()
        for version, expr in sorted(crates[name].items()):
            license = chosen_license(expr)
            if license is None:
                errors.append(
                    f"{name} {version}: no rule for license '{expr}'; "
                    "see the docstring of this script"
                )
            else:
                chosen.add(license)
        if len(chosen) > 1:
            errors.append(f"{name}: its versions are used under {sorted(chosen)}")
        for license in chosen:
            by_license[license].append(name)

    text = HEADER
    for key, heading in SECTIONS:
        names = by_license[key]
        if not names:
            continue
        text += f"\n{RULE}\n{heading} ({key})\n\n"
        text += "".join(f"    {n}\n" for n in names)
    return text, by_license


def main():
    fix = sys.argv[1:] == ["--fix"]
    if sys.argv[1:] not in ([], ["--fix"]):
        sys.exit(__doc__.split("\n\n")[-1])

    errors = []
    crates = linked_crates()
    sources = source_dirs()
    text, by_license = appendix(crates, errors)

    with open(LICENSE) as f:
        expected = f.read() + text
    with open(LICENSE_BINARY) as f:
        actual = f.read()
    if actual != expected:
        if fix:
            with open(LICENSE_BINARY, "w") as f:
                f.write(expected)
            print("Rewrote LICENSE-binary")
        else:
            errors.append(
                "LICENSE-binary does not match LICENSE plus the generated "
                "crate list; run with --fix and review the diff"
            )

    with open(NOTICE) as f, open(NOTICE_BINARY) as g:
        if f.read() != g.read():
            errors.append("NOTICE-binary differs from NOTICE")

    needed = {
        name for key, names in by_license.items() if key != "Apache-2.0"
        for name in names
    }
    for name in sorted(needed):
        origin = LICENSE_FROM.get(name, name)
        if origin not in crates:
            errors.append(f"{name}: {origin} is not linked, so it has no source")
            continue
        dest = os.path.join(LICENSES_DIR, name)
        for version in sorted(crates[origin]):
            src = sources[(origin, version)]
            wanted = license_files(src)
            if not wanted:
                errors.append(
                    f"{origin} {version} has no license file; add it to "
                    "LICENSE_FROM"
                )
                continue
            have = sorted(os.listdir(dest)) if os.path.isdir(dest) else []
            stale = have != wanted or any(
                open(os.path.join(src, n), "rb").read()
                != open(os.path.join(dest, n), "rb").read()
                for n in wanted
            )
            if not stale:
                continue
            if fix:
                shutil.rmtree(dest, ignore_errors=True)
                os.makedirs(dest)
                for n in wanted:
                    shutil.copyfile(os.path.join(src, n), os.path.join(dest, n))
                print(f"Copied licenses-binary/{name}/ from {origin} {version}: "
                      f"{', '.join(wanted)}")
            else:
                errors.append(
                    f"licenses-binary/{name}/ does not match the license files "
                    f"of {origin} {version} ({', '.join(wanted)})"
                )

    for name in sorted(os.listdir(LICENSES_DIR)):
        if name in needed or name in STD_DIRS:
            continue
        if fix:
            shutil.rmtree(os.path.join(LICENSES_DIR, name))
            print(f"Removed licenses-binary/{name}/")
        else:
            errors.append(f"licenses-binary/{name}/ is not needed by any crate")
    for name in sorted(STD_DIRS):
        if not os.path.isdir(os.path.join(LICENSES_DIR, name)):
            errors.append(f"licenses-binary/{name}/ is missing")

    for e in errors:
        print(f"ERROR: {e}", file=sys.stderr)
    if errors:
        sys.exit(1)
    if not fix:
        print(f"OK: {len(crates)} crates, {len(needed)} with license files")


if __name__ == "__main__":
    main()
