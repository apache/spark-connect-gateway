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

//! gRPC proxy for the Spark Connect service.
//!
//! Request flow:
//!
//! 1. Accept inbound `spark.connect.SparkConnectService` traffic via tonic.
//! 2. For each RPC, derive a [`SessionKey`] (and an `operation_id` where
//!    applicable) from the request.
//! 3. Ask the [`Router`] for the backend address. The router consults the
//!    affinity store first; on miss, it picks from the pool and records the
//!    decision so subsequent calls stick to the same backend.
//! 4. Open or reuse a tonic [`Channel`] to that backend (via [`Dialer`]),
//!    forward the request, and pump any response stream back to the client.
//!
//! All RPCs in the Spark Connect surface are forwarded explicitly; new
//! RPCs added by upstream Spark will surface here as `Unimplemented`
//! until they are wired in. There is no generic tower-level
//! passthrough.

mod config_filter;
mod dial;
mod handler;
mod outbound;

pub use dial::Dialer;
pub use handler::SparkConnectProxy;
pub use outbound::BackendTokens;
