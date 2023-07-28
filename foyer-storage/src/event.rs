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

use std::fmt::Debug;

use async_trait::async_trait;
use foyer_common::code::{Key, Value};

use crate::error::Result;

#[allow(unused_variables)]
#[async_trait]
pub trait EventListener: Send + Sync + 'static + Debug {
    type K: Key;
    type V: Value;

    async fn on_recover(&self, key: &Self::K) -> Result<()> {
        Ok(())
    }

    async fn on_insert(&self, key: &Self::K) -> Result<()> {
        Ok(())
    }

    async fn on_remove(&self, key: &Self::K) -> Result<()> {
        Ok(())
    }

    async fn on_evict(&self, key: &Self::K) -> Result<()> {
        Ok(())
    }

    async fn on_clear(&self) -> Result<()> {
        Ok(())
    }
}
