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

/// Some phantom metrics components that do nothing.
pub mod noop;

/// Prometheus metrics components.
#[cfg(feature = "prometheus")]
pub mod prometheus;

/// Prometheus metrics components.
#[cfg(feature = "prometheus-client")]
pub use prometheus_client_0_22 as prometheus_client;

/// Prometheus metrics components.
#[cfg(feature = "prometheus-client_0_22")]
pub mod prometheus_client_0_22;

#[cfg(feature = "opentelemetry")]
pub use opentelemetry_0_27 as opentelemetry;

/// OpenTelemetry metrics components.
#[cfg(feature = "opentelemetry_0_27")]
pub mod opentelemetry_0_27;

/// OpenTelemetry metrics components.
#[cfg(feature = "opentelemetry_0_26")]
pub mod opentelemetry_0_26;
