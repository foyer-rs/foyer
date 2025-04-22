// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{future::Future, net::SocketAddr, pin::Pin};

use anyhow::Ok;
use foyer::{Cache, CacheBuilder};
use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
    header::CONTENT_TYPE,
    server::conn::http1,
    service::Service,
};
use hyper_util::rt::TokioIo;
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use prometheus::{Encoder, Registry, TextEncoder};
use tokio::net::TcpListener;

pub struct PrometheusExporter {
    registry: Registry,
    addr: SocketAddr,
}

impl PrometheusExporter {
    pub fn new(registry: Registry, addr: SocketAddr) -> Self {
        Self { registry, addr }
    }

    pub fn run(self) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&self.addr).await.unwrap();
            loop {
                let (stream, _) = match listener.accept().await {
                    Result::Ok(res) => res,
                    Err(e) => {
                        tracing::error!("[prometheus exporter]: accept connection error: {e}");
                        continue;
                    }
                };

                let io = TokioIo::new(stream);
                let handle = Handle {
                    registry: self.registry.clone(),
                };

                tokio::spawn(async move {
                    if let Err(e) = http1::Builder::new().serve_connection(io, handle).await {
                        tracing::error!("[prometheus exporter]: serve request error: {e}");
                    }
                });
            }
        });
    }
}

struct Handle {
    registry: Registry,
}

impl Service<Request<Incoming>> for Handle {
    type Response = Response<Full<Bytes>>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, _: Request<Incoming>) -> Self::Future {
        let mfs = self.registry.gather();

        Box::pin(async move {
            let encoder = TextEncoder::new();
            let mut buffer = vec![];
            encoder.encode(&mfs, &mut buffer)?;

            Ok(Response::builder()
                .status(200)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(Full::new(Bytes::from(buffer)))
                .unwrap())
        })
    }
}

#[tokio::main]
async fn main() {
    // Create a new registry or use the global registry of `prometheus` lib.
    let registry = Registry::new();

    // Create a `PrometheusExporter` powered by hyper and run it.
    let addr = "127.0.0.1:19970".parse().unwrap();
    PrometheusExporter::new(registry.clone(), addr).run();

    // Build a cache with `PrometheusMetricsRegistry`.
    let _: Cache<String, String> = CacheBuilder::new(100)
        .with_metrics_registry(Box::new(PrometheusMetricsRegistry::new(registry)))
        .build();

    // > curl http://127.0.0.1:7890
    //
    // # HELP foyer_hybrid_op_duration foyer hybrid cache operation durations
    // # TYPE foyer_hybrid_op_duration histogram
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="0.005"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="0.01"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="0.025"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="0.05"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="0.1"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="0.25"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="0.5"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="1"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="2.5"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="5"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="10"} 0
    // foyer_hybrid_op_duration_bucket{name="foyer",op="fetch",le="+Inf"} 0
    // ... ...
}
