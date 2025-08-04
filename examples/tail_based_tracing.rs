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

use std::time::Duration;

use foyer::{
    DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder, LargeObjectEngineBuilder, TracingOptions,
};

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
    use std::borrow::Cow;

    use fastrace::collector::Config;
    use fastrace_opentelemetry::OpenTelemetryReporter;
    use opentelemetry::{InstrumentationScope, KeyValue};
    use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
    use opentelemetry_sdk::Resource;

    let reporter = OpenTelemetryReporter::new(
        SpanExporter::builder()
            .with_tonic()
            .with_protocol(Protocol::Grpc)
            .with_endpoint("http://127.0.0.1:4317")
            .build()
            .unwrap(),
        Cow::Owned(
            Resource::builder()
                .with_attributes([KeyValue::new("service.name", "foyer")])
                .build(),
        ),
        InstrumentationScope::builder("foyer")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build(),
    );
    fastrace::set_reporter(reporter, Config::default());
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

    let device = FsDeviceBuilder::new(dir.path())
        .with_capacity(256 * 1024 * 1024)
        .build()?;

    let hybrid: HybridCache<u64, String> = HybridCacheBuilder::new()
        .memory(64 * 1024 * 1024)
        .storage()
        .with_engine_builder(LargeObjectEngineBuilder::new(device))
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
