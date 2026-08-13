// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generated tonic bindings for the upstream `spark.connect.*` proto surface.
//!
//! The proto files under `proto/spark/connect/` are a read-only mirror of the
//! corresponding files in `apache/spark`. Bindings are regenerated at build
//! time by `build.rs`.
//!
//! Lints disabled here are intrinsic to prost-generated code, not bugs we
//! should fix:
//! * `large_enum_variant` — Spark Connect oneofs are inherently sized by
//!   their largest variant; boxing every variant would be invasive and
//!   would make the proto API noticeably worse to use.
//! * `clippy::all` more broadly: generated code is not human-authored, so
//!   lint findings are not actionable.

#![allow(clippy::all)]
#![allow(clippy::pedantic)]

pub mod spark {
    pub mod connect {
        tonic::include_proto!("spark.connect");
    }
}

pub use spark::connect as pb;
