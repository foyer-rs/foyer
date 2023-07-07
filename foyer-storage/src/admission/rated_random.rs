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

use foyer_common::code::{Key, Value};
use parking_lot::{Mutex, MutexGuard};
use rand::{thread_rng, Rng};

use super::AdmissionPolicy;

const PRECISION: usize = 100000;

/// p       : admit probability among △t
/// w(t)    : weight at time t
/// r       : actual admitted rate
/// E(w)    : expected admitted weight
/// E(r)    : expceted admitted rate
///
/// E(w(t)) = p * w(t)
/// r = sum(△t){ admitted w(t) } / △t
/// E(r) = sum(△t){ E(w(t)) } / △t
///      = sum(△t){ p * w(t) } / △t
///      = p / △t * sum(△t){ w(t) }
/// p = E(r) * △t / sum(△t){ w(t) }
pub struct RatedRandom<K, V>
where
    K: Key,
    V: Value,
{
    rate: usize,
    update_interval: Duration,

    bytes: AtomicUsize,
    probability: AtomicUsize,

    inner: Mutex<Inner<K, V>>,
}

impl<K, V> Debug for RatedRandom<K, V>
where
    K: Key,
    V: Value,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicRandom").finish()
    }
}

struct Inner<K, V>
where
    K: Key,
    V: Value,
{
    last_update_time: Option<Instant>,
    last_bytes: usize,

    _marker: PhantomData<(K, V)>,
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
            bytes: AtomicUsize::new(0),
            probability: AtomicUsize::new(0),
            inner: Mutex::new(Inner {
                last_update_time: None,
                last_bytes: 0,
                _marker: PhantomData,
            }),
        }
    }

    fn judge(&self, key: &K, value: &V) -> bool {
        if let Some(inner) = self.inner.try_lock() {
            self.update(inner);
        }

        // TODO(MrCroxx): unify weighter?
        let weight = key.serialized_len() + value.serialized_len();
        self.bytes.fetch_add(weight, Ordering::Relaxed);

        thread_rng().gen_range(0..PRECISION) < self.probability.load(Ordering::Relaxed)
    }

    fn update(&self, mut inner: MutexGuard<'_, Inner<K, V>>) {
        let now = Instant::now();

        let elapsed = match inner.last_update_time {
            Some(last_update_time) => now.duration_since(last_update_time),
            None => self.update_interval,
        };

        if elapsed < self.update_interval {
            return;
        }

        let bytes = self.bytes.load(Ordering::Relaxed);
        let bytes_delta = std::cmp::max(bytes - inner.last_bytes, 1);

        let p = self.rate as f64 * elapsed.as_secs_f64() / bytes_delta as f64;
        let p = (p * PRECISION as f64) as usize;
        self.probability.store(p, Ordering::Relaxed);

        inner.last_update_time = Some(now);
        inner.last_bytes = bytes;
    }
}

impl<K, V> AdmissionPolicy for RatedRandom<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;

    type Value = V;

    fn judge(&self, key: &Self::Key, value: &Self::Value) -> bool {
        self.judge(key, value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_rated_random() {
        const RATE: usize = 1_000_000;
        const ERATIO: f64 = 0.1;

        let score = Arc::new(AtomicUsize::new(0));

        let rr = Arc::new(RatedRandom::<u64, Vec<u8>>::new(
            RATE,
            Duration::from_millis(100),
        ));

        async fn submit(rr: Arc<RatedRandom<u64, Vec<u8>>>, score: Arc<AtomicUsize>) {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let size = thread_rng().gen_range(1000..10000);
                if rr.judge(&0, &vec![0; size]) {
                    score.fetch_add(size, Ordering::Relaxed);
                }
            }
        }

        for _ in 0..10 {
            tokio::spawn(submit(rr.clone(), score.clone()));
        }

        tokio::time::sleep(Duration::from_secs(10)).await;
        let s = score.load(Ordering::Relaxed);
        let error = (s as isize - RATE as isize * 10).unsigned_abs();
        let eratio = error as f64 / (RATE as f64 * 10.0);

        assert!(eratio < ERATIO);
    }
}
