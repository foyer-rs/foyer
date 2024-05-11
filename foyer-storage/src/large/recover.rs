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

use std::collections::HashMap;
use std::hash::BuildHasher;

use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use foyer_common::code::{StorageKey, StorageValue};
use futures::future::try_join_all;

use itertools::Itertools;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

use crate::catalog::{AtomicSequence, Sequence};
use crate::error::{Error, Result};
use crate::large::scanner::{EntryInfo, RegionScanner};

use super::generic::GenericStoreConfig;
use super::indexer::Indexer;
use super::region::RegionManager;
use super::tombstone::Tombstone;
use super::{
    device::{Device, DeviceExt, RegionId},
    indexer::EntryAddress,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverMode {
    // TODO(MrCroxx): Update `NoRecovery` docs after thombstone is supported.
    /// Do not recover disk cache.
    ///
    /// For updatable cache, [`RecoverMode::NoRecovery`] must be used to prevent from phantom entry after reopen.
    NoRecovery,
    /// Recover disk cache and skip errors.
    QuietRecovery,
    /// Recover disk cache and panic on errors.
    StrictRecovery,
}

#[derive(Debug)]
pub struct RecoverRunner;

impl RecoverRunner {
    pub async fn run<K, V, S, D>(
        config: &GenericStoreConfig<K, V, S, D>,
        device: D,
        sequence: &AtomicSequence,
        indexer: &Indexer,
        region_manager: &RegionManager<D>,
        tombstones: &[Tombstone],
        runtime: Handle,
    ) -> Result<()>
    where
        K: StorageKey,
        V: StorageValue,
        S: BuildHasher + Send + Sync + 'static + Debug,
        D: Device,
    {
        // Recover regions concurrently.
        let semaphore = Arc::new(Semaphore::new(config.recover_concurrency));
        let mode = config.recover_mode;
        let handles = (0..device.regions() as RegionId).map(|region| {
            let device = device.clone();
            let semaphore = semaphore.clone();
            runtime.spawn(async move {
                let permit = semaphore.acquire().await;
                let res = RegionRecoverRunner::run(mode, device, region).await;
                drop(permit);
                res
            })
        });
        let total = try_join_all(handles).await.unwrap();

        // Return error is there is.
        let (total, errs): (Vec<_>, Vec<_>) = total.into_iter().partition(|res| res.is_ok());
        if !errs.is_empty() {
            let errs = errs.into_iter().map(|r| r.unwrap_err()).collect_vec();
            return Err(Error::multiple(errs));
        }

        #[derive(Debug)]
        enum EntryAddressOrTombstoneHash {
            EntryAddress(EntryAddress),
            Tombstone,
        }

        // Dedup entries.
        let mut latest_sequence = 0;
        let mut indices: HashMap<u64, Vec<(Sequence, EntryAddressOrTombstoneHash)>> = HashMap::new();
        let mut clean_regions = vec![];
        let mut evictable_regions = vec![];
        for (region, infos) in total.into_iter().map(|r| r.unwrap()).enumerate() {
            let region = region as RegionId;

            if infos.is_empty() {
                clean_regions.push(region);
            } else {
                evictable_regions.push(region);
            }

            for EntryInfo { hash, sequence, addr } in infos {
                latest_sequence = latest_sequence.max(sequence);
                indices
                    .entry(hash)
                    .or_default()
                    .push((sequence, EntryAddressOrTombstoneHash::EntryAddress(addr)));
            }
        }
        tombstones.iter().for_each(|tombstone| {
            latest_sequence = latest_sequence.max(tombstone.sequence);
            indices
                .entry(tombstone.hash)
                .or_default()
                .push((tombstone.sequence, EntryAddressOrTombstoneHash::Tombstone))
        });
        let indices = indices
            .into_iter()
            .filter_map(|(hash, mut versions)| {
                versions.sort_by_key(|(sequence, _)| *sequence);
                tracing::trace!("[recover runner]: hash {hash} has versions: {versions:?}");
                match versions.pop() {
                    None => None,
                    Some((_, EntryAddressOrTombstoneHash::Tombstone)) => None,
                    Some((_, EntryAddressOrTombstoneHash::EntryAddress(addr))) => Some((hash, addr)),
                }
            })
            .collect_vec();
        // let indices = indices.into_iter().map(|(hash, (_, addr))| (hash, addr)).collect_vec();
        let permits = config.clean_region_threshold.saturating_sub(clean_regions.len());
        let countdown = clean_regions.len().saturating_sub(config.clean_region_threshold);

        // Log recovery.
        tracing::info!(
            "Recovers {e} regions with data, {c} clean regions, {t} total entries with max sequence as {s}, initial reclaim permits is {p}.",
            e = evictable_regions.len(),
            c = clean_regions.len(),
            t = indices.len(),
            s = latest_sequence,
            p = permits,
        );

        // Update components.
        indexer.insert_batch(indices);
        sequence.store(latest_sequence + 1, Ordering::Release);
        for region in clean_regions {
            region_manager.mark_clean(region).await;
        }
        for region in evictable_regions {
            region_manager.mark_evictable(region);
        }
        region_manager.reclaim_semaphore().add_permits(permits);
        region_manager.reclaim_semaphore_countdown().reset(countdown);

        // Note: About reclaim semaphore permits and countdown:
        //
        // ```
        // permits = clean region threshold - clean region - reclaiming region
        // ```
        //
        // When recovery, `reclaiming region` is always `0`.
        //
        // If `clean region threshold >= clean region`, permits is simply `clean region threshold - clean region`.
        //
        // If `clean region threshold < clean region`, for permits must be NON-NEGATIVE, we can temporarily set permits
        // to `0`, and skip first `clean region - clean region threshold` permits increments. It is implemented by
        // `Countdown`.

        Ok(())
    }
}

#[derive(Debug)]
struct RegionRecoverRunner;

impl RegionRecoverRunner {
    async fn run<D>(mode: RecoverMode, device: D, region: RegionId) -> Result<Vec<EntryInfo>>
    where
        D: Device,
    {
        assert_ne!(mode, RecoverMode::NoRecovery);

        let mut infos = vec![];

        let mut iter = RegionScanner::new(region, device);
        loop {
            let r = iter.next().await;
            match r {
                Err(e) => {
                    if mode == RecoverMode::StrictRecovery {
                        return Err(e);
                    } else {
                        tracing::warn!(
                            "error raised when recovering region {region}, skip further recovery for region {region}"
                        );
                        break;
                    }
                }
                Ok(Some(info)) => infos.push(info),
                Ok(None) => break,
            }
        }

        Ok(infos)
    }
}
