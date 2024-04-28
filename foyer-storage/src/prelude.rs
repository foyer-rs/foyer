//  Copyright 2024 Foyer Project Authors
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

pub use crate::{
    admission::{rated_ticket::RatedTicketAdmissionPolicy, AdmissionContext, AdmissionPolicy},
    compress::Compression,
    device::fs::{FsDeviceConfig, FsDeviceConfigBuilder},
    error::{Error, Result},
    generic::RecoverMode,
    metrics::{get_metrics_registry, set_metrics_registry},
    reinsertion::{
        exist::ExistReinsertionPolicy, rated_ticket::RatedTicketReinsertionPolicy, ReinsertionContext,
        ReinsertionPolicy,
    },
    runtime::{RuntimeConfig, RuntimeConfigBuilder, RuntimeStoreConfig},
    storage::{AsyncStorageExt, CachedEntry, ForceStorageExt, Storage, StorageExt, StorageWriter},
    store::{DeviceConfig, FsStoreConfig, Store, StoreBuilder, StoreConfig, StoreWriter},
};
