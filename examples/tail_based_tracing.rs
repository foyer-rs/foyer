//  Copyright 2024 foyer Project Authors
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

use std::time::Duration;

use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder, TracingOptions};

#[cfg(feature = "jaeger")]
fn init_jaeger_exporter() {
    let reporter = fastrace_jaeger::JaegerReporter::new("127.0.0.1:6831".parse().unwrap(), "example").unwrap();
    fastrace::set_reporter(
        reporter,
        fastrace::collector::Config::default().report_interval(Duration::from_millis(1)),
    );
}

#[cfg(feature = "ot")]
fn init_opentelemetry_exporter() {
    use opentelemetry_otlp::WithExportConfig;

    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://127.0.0.1:4317")
        .with_protocol(opentelemetry_otlp::Protocol::Grpc)
        .with_timeout(Duration::from_secs(
            opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
        ))
        .build_span_exporter()
        .unwrap();
    let reporter = fastrace_opentelemetry::OpenTelemetryReporter::new(
        exporter,
        opentelemetry::trace::SpanKind::Server,
        std::borrow::Cow::Owned(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            "example",
        )])),
        opentelemetry::InstrumentationLibrary::builder("opentelemetry-instrumentation-foyer").build(),
    );
    fastrace::set_reporter(reporter, fastrace::collector::Config::default());
}

fn init_exporter() {
    #[cfg(feature = "jaeger")]
    init_jaeger_exporter();

    #[cfg(feature = "ot")]
    init_opentelemetry_exporter();

    #[cfg(not(any(feature = "jaeger", feature = "ot")))]
    panic!("Either jaeger or opentelemetry feature must be enabled!");
}

/// NOTE: To run this example, please enable feature "tracing" and either "jaeger" or "ot".
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_exporter();

    let dir = tempfile::tempdir()?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .memory(64 * 1024 * 1024)
        .storage(Engine::Large)
        .with_device_options(DirectFsDeviceOptions::new(dir.path()).with_capacity(256 * 1024 * 1024))
        .build()
        .await?;

    hybrid.enable_tracing();
    hybrid.update_tracing_options(TracingOptions::new().with_record_hybrid_get_threshold(Duration::from_millis(10)));

    hybrid.insert(42, "The answer to life, the universe, and everything.".to_string());
    assert_eq!(
        hybrid.get(&42).await?.unwrap().value(),
        "The answer to life, the universe, and everything."
    );

    Ok(())
}
