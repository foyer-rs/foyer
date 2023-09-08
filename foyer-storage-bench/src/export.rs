//  Copyright 2023 MrCroxx
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::net::SocketAddr;

use foyer_storage::metrics::get_metrics_registry;
use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Error, Request, Response, Server,
};
use prometheus::{Encoder, TextEncoder};

pub struct MetricsExporter;

impl MetricsExporter {
    pub fn init(addr: SocketAddr) {
        tokio::spawn(async move {
            tracing::info!("Prometheus service is set up on http://{}", addr);
            if let Err(e) = Server::bind(&addr)
                .serve(make_service_fn(|_| async move {
                    Ok::<_, Error>(service_fn(Self::serve))
                }))
                .await
            {
                tracing::error!("Prometheus service error: {}", e);
            }
        });
    }

    async fn serve(_request: Request<Body>) -> anyhow::Result<Response<Body>> {
        let encoder = TextEncoder::new();
        let mut buffer = Vec::with_capacity(4096);
        let metrics = get_metrics_registry().gather();
        encoder.encode(&metrics, &mut buffer)?;
        let response = Response::builder()
            .status(200)
            .header(CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))?;
        Ok(response)
    }
}
