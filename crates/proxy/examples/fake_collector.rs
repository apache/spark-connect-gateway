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

//! Minimal OTLP/gRPC trace receiver. Listens on `:4317`, accepts
//! `ExportTraceServiceRequest`, prints span name + selected attributes
//! for each span, and exits as soon as it has seen `--expect <name>`
//! at least once (or after `--timeout-secs` if set).
//!
//! Used by the e2e harness to validate that the production gateway
//! actually emits the `scg_rpc` span via OTLP/gRPC.

use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

fn bytes_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        use std::fmt::Write;
        write!(&mut s, "{:02x}", byte).unwrap();
    }
    s
}

#[derive(Clone)]
struct Collector {
    seen_names: Arc<Mutex<HashSet<String>>>,
    expect: Option<String>,
    done_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let body = req.into_inner();
        for rs in &body.resource_spans {
            // Resource attributes (service.name etc.)
            if let Some(res) = &rs.resource {
                let svc = res
                    .attributes
                    .iter()
                    .find(|kv| kv.key == "service.name")
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| match v.value.as_ref() {
                        Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                s,
                            ),
                        ) => Some(s.as_str()),
                        _ => None,
                    });
                if let Some(s) = svc {
                    println!("[collector] resource service.name={}", s);
                }
            }
            for ss in &rs.scope_spans {
                for span in &ss.spans {
                    let attrs: Vec<(String, String)> = span
                        .attributes
                        .iter()
                        .filter_map(|kv| {
                            kv.value.as_ref().and_then(|v| match v.value.as_ref()? {
                                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
                                    Some((kv.key.clone(), s.clone()))
                                }
                                _ => None,
                            })
                        })
                        .collect();
                    let trace_id = bytes_hex(&span.trace_id);
                    let span_id = bytes_hex(&span.span_id);
                    let parent_id = if span.parent_span_id.is_empty() {
                        "(root)".to_string()
                    } else {
                        bytes_hex(&span.parent_span_id)
                    };
                    println!(
                        "[collector] span name={} trace_id={} span_id={} parent={} attrs={:?}",
                        span.name, trace_id, span_id, parent_id, attrs
                    );

                    let mut seen = self.seen_names.lock().await;
                    seen.insert(span.name.clone());
                    if let Some(expected) = &self.expect {
                        if seen.contains(expected) {
                            // Trigger shutdown if first time we see it.
                            let mut tx = self.done_tx.lock().await;
                            if let Some(s) = tx.take() {
                                println!(
                                    "[collector] saw expected span name={}; shutting down",
                                    expected
                                );
                                let _ = s.send(());
                            }
                        }
                    }
                }
            }
        }
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mut expect: Option<String> = None;
    let mut timeout_secs: u64 = 30;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--expect" => expect = args.next(),
            "--timeout-secs" => {
                timeout_secs = args.next().and_then(|s| s.parse().ok()).unwrap_or(30)
            }
            other => eprintln!("[collector] ignoring unknown arg: {}", other),
        }
    }

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let svc = Collector {
        seen_names: Arc::new(Mutex::new(HashSet::new())),
        expect: expect.clone(),
        done_tx: Arc::new(Mutex::new(Some(done_tx))),
    };

    let addr = "0.0.0.0:4317".parse()?;
    eprintln!(
        "[collector] listening on {}; expect={:?} timeout_secs={}",
        addr, expect, timeout_secs
    );

    let server = Server::builder()
        .add_service(TraceServiceServer::new(svc))
        .serve_with_shutdown(addr, async move {
            tokio::select! {
                _ = done_rx => {}
                _ = tokio::time::sleep(Duration::from_secs(timeout_secs)) => {
                    eprintln!("[collector] timeout reached; shutting down");
                }
            }
        });
    server.await?;
    Ok(())
}
