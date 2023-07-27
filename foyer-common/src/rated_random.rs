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

use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use crate::code::{Key, Value};
use parking_lot::{Mutex, MutexGuard};
use rand::{thread_rng, Rng};

const PRECISION: usize = 100000;

/// p       : admit probability among △t
/// w_t    :  entry weight at time t
/// r       : actual admitted rate
/// E(w)    : expected admitted weight
/// E(r)    : expceted admitted rate
///
/// E(w_t) = {
///   p * w  ( if  judge &&  insert )
///   p * w  ( if !judge && !insert )
///   w      ( if !judge &&  insert )
///   0      ( if  judge && !insert )
/// }
///
///    E(r) = sum_{△t}{E(w_t)} / △t
/// => E(r) * △t = sum_{△t}{ E(w_t) }
/// => E(r) * △t =   sum_{△t}^{(judge && insert) || (!judge && !insert)}{ p * w_t }
///                + sum_{△t}^{(!judge && insert)}{ w_t }
///                + sum_{△t}^{(judge && !insert){ 0 }
/// => E(r) * △t) - sum_{△t}^{(!judge && insert)}{ w_t } = p * sum_{△t}^{(judge && insert) || (!judge && !insert)}{ w_t }
/// => p = (E(r) * △t) - sum_{△t}^{(!judge && insert)}{ w_t }) / (sum_{△t}^{(judge && insert) || (!judge && !insert)}{ w_t })
///
/// p = (E(r) * △t) - sum_{△t}^{(!judge && insert)}{ w_t }) / (sum_{△t}^{(judge && insert) || (!judge && !insert)}{ w_t })
///     ↑ rate        ↑ △force_insert_bytes                   ↑ △obey_bytes
///
/// p = ( rate * △t - △force_insert_bytes ) / △obey_bytes
#[derive(Debug)]
pub struct RatedRandom<K, V>
where
    K: Key,
    V: Value,
{
    rate: usize,
    update_interval: Duration,

    obey_bytes: AtomicUsize,
    force_inser_bytes: AtomicUsize,

    probability: AtomicUsize,

    inner: Mutex<Inner>,

    _marker: PhantomData<(K, V)>,
}

#[derive(Debug)]
struct Inner {
    last_update_time: Option<Instant>,
    last_obey_bytes: usize,
    last_force_insert_bytes: usize,
}

impl<K, V> RatedRandom<K, V>
where
    K: Key,
    V: Value,
{
    pub fn new(rate: usize, update_interval: Duration) -> Self {
        Self {
            rate,
            update_interval,

            obey_bytes: AtomicUsize::new(0),
            force_inser_bytes: AtomicUsize::new(0),

            probability: AtomicUsize::new(0),
            inner: Mutex::new(Inner {
                last_update_time: None,
                last_obey_bytes: 0,
                last_force_insert_bytes: 0,
            }),
            _marker: PhantomData,
        }
    }

    pub fn judge(&self, _key: &K, _weight: usize) -> bool {
        if let Some(inner) = self.inner.try_lock() {
            self.update(inner);
        }

        thread_rng().gen_range(0..PRECISION) < self.probability.load(Ordering::Relaxed)
    }

    pub fn on_insert(&self, _key: &K, weight: usize, judge: bool) {
        if judge {
            // obey
            self.obey_bytes.fetch_add(weight, Ordering::Relaxed);
        } else {
            // force insert
            self.force_inser_bytes.fetch_add(weight, Ordering::Relaxed);
        }
    }

    pub fn on_drop(&self, _key: &K, weight: usize, judge: bool) {
        if !judge {
            // obey
            self.obey_bytes.fetch_add(weight, Ordering::Relaxed);
        }
    }

    fn update(&self, mut inner: MutexGuard<'_, Inner>) {
        // p = ( rate * △t - △force_insert_bytes ) / △obey_bytes

        let now = Instant::now();

        let elapsed = match inner.last_update_time {
            Some(last_update_time) => now.duration_since(last_update_time),
            None => self.update_interval,
        };

        if elapsed < self.update_interval {
            return;
        }

        let now_obey_bytes = self.obey_bytes.load(Ordering::Relaxed);
        let now_force_insert_bytes = self.force_inser_bytes.load(Ordering::Relaxed);

        let delta_obey_bytes = now_obey_bytes - inner.last_obey_bytes;
        let delta_force_insert_bytes = now_force_insert_bytes - inner.last_force_insert_bytes;

        inner.last_update_time = Some(now);
        inner.last_obey_bytes = now_obey_bytes;
        inner.last_force_insert_bytes = now_force_insert_bytes;

        let p = if delta_obey_bytes == 0 {
            1.0
        } else {
            let numerator =
                self.rate as f64 * elapsed.as_secs_f64() - delta_force_insert_bytes as f64;
            let numerator = numerator.abs();
            let p = numerator / delta_obey_bytes as f64;
            p.min(1.0)
        };

        debug_assert!((0.0..=1.0).contains(&p), "p out of range 0..=1: {}", p);

        let p = (p * PRECISION as f64) as usize;
        self.probability.store(p, Ordering::Relaxed);

        tracing::debug!("probability: {}", p as f64 / PRECISION as f64);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_rated_random() {
        const CASES: usize = 10;
        const ERATIO: f64 = 0.1;

        let handles = (0..CASES).map(|_| tokio::spawn(case())).collect_vec();
        let mut eratios = vec![];
        for handle in handles {
            let eratio = handle.await.unwrap();
            assert!(eratio < ERATIO, "eratio: {} < ERATIO: {}", eratio, ERATIO);
            eratios.push(eratio);
        }
        println!("========== RatedRandom error ratio begin ==========");
        for eratio in eratios {
            println!("eratio: {eratio}");
        }
        println!("=========== RatedRandom error ratio end ===========");
    }

    async fn case() -> f64 {
        const RATE: usize = 1_000_000;
        const CONCURRENCY: usize = 10;

        const P_OTHER: f64 = 0.8;
        const P_FORCE: f64 = 0.1;

        let score = Arc::new(AtomicUsize::new(0));

        let rr = Arc::new(RatedRandom::<u64, Vec<u8>>::new(
            RATE,
            Duration::from_millis(100),
        ));

        // scope: CONCURRENCY * (1 / interval) * range
        // [1_000_000, 10_000_000]
        // FORCE: [100_000, 1_000_000]

        async fn submit(rr: Arc<RatedRandom<u64, Vec<u8>>>, score: Arc<AtomicUsize>) {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let weight = thread_rng().gen_range(100..1000);

                let judge = rr.judge(&0, weight);
                let p_other = thread_rng().gen_range(0.0..=1.0);
                let p_force = thread_rng().gen_range(0.0..=1.0);

                let insert = (p_force <= P_FORCE) || (p_other <= P_OTHER && judge);

                if insert {
                    score.fetch_add(weight, Ordering::Relaxed);
                    rr.on_insert(&0, weight, judge);
                } else {
                    rr.on_drop(&0, weight, judge);
                }
            }
        }

        for _ in 0..CONCURRENCY {
            tokio::spawn(submit(rr.clone(), score.clone()));
        }

        tokio::time::sleep(Duration::from_secs(10)).await;
        let s = score.load(Ordering::Relaxed);
        let error = (s as isize - RATE as isize * 10).unsigned_abs();
        error as f64 / (RATE as f64 * 10.0)
    }
}
