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

//! Tiny e2e helper used to validate the OTel exporter path against a
//! real gateway + real OTLP collector. Sends one Config RPC.
//!
//! Run with:
//! ```
//! cargo run -p scg-proxy --example e2e_client -- http://127.0.0.1:15003
//! ```
//!
//! The RPC is expected to FAIL (the e2e config points the gateway at an
//! unreachable backend) — but the gateway's `scg_rpc` span fires
//! regardless, which is what we want to observe at the collector.

use scg_genproto::pb;
use std::env;
use tonic::metadata::MetadataValue;
use tonic::Request;

#[tokio::main]
async fn main() {
    let mut url = "http://127.0.0.1:15003".to_string();
    let mut traceparent: Option<String> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--traceparent" => traceparent = args.next(),
            other if other.starts_with("http") => url = other.to_string(),
            other => eprintln!("[e2e_client] ignoring arg: {}", other),
        }
    }
    eprintln!(
        "[e2e_client] connecting to gateway at {} (traceparent={:?})",
        url, traceparent
    );

    let ch = tonic::transport::Endpoint::from_shared(url)
        .unwrap()
        .connect()
        .await
        .expect("connect to gateway");
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);

    let mut req = Request::new(pb::ConfigRequest {
        session_id: "e2e-session".into(),
        ..Default::default()
    });
    if let Some(tp) = traceparent {
        req.metadata_mut()
            .insert("traceparent", MetadataValue::try_from(tp).unwrap());
    }
    let res = c.config(req).await;
    match res {
        Ok(resp) => eprintln!("[e2e_client] OK: {:?}", resp),
        Err(s) => eprintln!(
            "[e2e_client] RPC failed (expected): code={:?} message={}",
            s.code(),
            s.message()
        ),
    }
}
