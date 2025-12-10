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
use http_body_util::Full;
use hyper::{
    body::{Bytes, Incoming},
    header::CONTENT_TYPE,
    server::conn::http1,
    service::Service,
    Request, Response,
};
use hyper_util::rt::TokioIo;
use prometheus::{Encoder, Registry, TextEncoder};
use foyer_common::tokio::net::TcpListener;

pub struct PrometheusExporter {
    registry: Registry,
    addr: SocketAddr,
}

impl PrometheusExporter {
    pub fn new(registry: Registry, addr: SocketAddr) -> Self {
        Self { registry, addr }
    }

    pub fn run(self) {
        foyer_common::tokio::spawn(async move {
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

                foyer_common::tokio::spawn(async move {
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
