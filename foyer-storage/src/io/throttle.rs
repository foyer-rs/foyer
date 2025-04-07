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

use std::time::Instant;

use parking_lot::Mutex;

#[derive(Debug)]
struct Inner {
    throughput_quota: f64,
    iops_quota: f64,

    last: Instant,
}

/// [`IoThrottler`] throttles IO operation in both iops and throughput aspects.
#[derive(Debug)]
pub struct IoThrottler {
    inner: Mutex<Inner>,

    throughput: f64,
    iops: f64,
}

impl IoThrottler {
    /// Create a new [`IoThrottler`] with the given throughput and iops.
    ///
    /// Note: Zero stands for umlimited.
    pub fn new(throughput: f64, iops: f64) -> Self {
        let inner = Inner {
            throughput_quota: 0.0,
            iops_quota: 0.0,
            last: Instant::now(),
        };
        Self {
            inner: Mutex::new(inner),
            throughput,
            iops,
        }
    }

    /// Check if there is still some quota left.
    pub fn probe(&self) -> bool {
        let mut inner = self.inner.lock();

        let now = Instant::now();
        let throughput_refill = now.duration_since(inner.last).as_secs_f64() * self.throughput;
        let iops_refill = now.duration_since(inner.last).as_secs_f64() * self.iops;
        inner.last = now;
        inner.throughput_quota = f64::min(inner.throughput_quota + throughput_refill, self.throughput);
        inner.iops_quota = f64::min(inner.iops_quota + iops_refill, self.iops);

        (self.throughput == 0.0 || inner.throughput_quota > 0.0) && (self.iops == 0.0 || inner.iops_quota > 0.0)
    }

    /// Reduce some throughput and iops quota manually.
    pub fn reduce(&self, throughput: f64, iops: f64) {
        self.inner.lock().throughput_quota -= throughput;
        self.inner.lock().iops_quota -= iops;
    }

    /// Consume some throughput and iops quota from the rate limiter.
    ///
    /// If there enough quota left, returns `true`; otherwise, returns `false`.
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn consume(&self, throughput: f64, iops: f64) -> bool {
        let mut inner = self.inner.lock();

        let now = Instant::now();
        let throughput_refill = now.duration_since(inner.last).as_secs_f64() * self.throughput;
        let iops_refill = now.duration_since(inner.last).as_secs_f64() * self.iops;
        inner.last = now;
        inner.throughput_quota = f64::min(inner.throughput_quota + throughput_refill, self.throughput);
        inner.iops_quota = f64::min(inner.iops_quota + iops_refill, self.iops);

        let enough =
            (self.throughput == 0.0 || inner.throughput_quota > 0.0) && (self.iops == 0.0 || inner.iops_quota > 0.0);

        if enough {
            if self.throughput > 0.0 {
                inner.throughput_quota -= throughput;
            }
            if self.iops > 0.0 {
                inner.iops_quota -= iops;
            }
        }

        enough
    }

    /// Get the throughput throttle.
    #[expect(dead_code)]
    pub fn throughput(&self) -> f64 {
        self.throughput
    }

    /// Get the iops throttle.
    #[expect(dead_code)]
    pub fn iops(&self) -> f64 {
        self.iops
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use itertools::Itertools;
    use rand::{rng, Rng};

    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum Target {
        Throughput,
        Iops,
    }

    #[ignore]
    #[test]
    fn test_rated_ticket_consume_throughput() {
        test(consume, Target::Throughput)
    }

    #[ignore]
    #[test]
    fn test_rated_ticket_consume_iops() {
        test(consume, Target::Iops)
    }

    #[ignore]
    #[test]
    fn test_rated_ticket_probe_reduce_throughput() {
        test(probe_reduce, Target::Throughput)
    }

    #[ignore]
    #[test]
    fn test_rated_ticket_probe_reduce_iops() {
        test(probe_reduce, Target::Iops)
    }

    fn test<F>(f: F, target: Target)
    where
        F: Fn(f64, f64, &Arc<AtomicUsize>, &Arc<AtomicUsize>, &Arc<IoThrottler>, Target) + Send + Sync + Copy + 'static,
    {
        const CASES: usize = 10;
        const ERATIO: f64 = 0.05;

        let handles = (0..CASES)
            .map(|_| std::thread::spawn(move || case(f, target)))
            .collect_vec();
        let mut eratios = vec![];
        for handle in handles {
            let eratio = handle.join().unwrap();
            assert!(eratio < ERATIO, "eratio: {} < ERATIO: {}", eratio, ERATIO);
            eratios.push(eratio);
        }
        println!("========== RatedTicket error ratio begin ==========");
        for eratio in eratios {
            println!("eratio: {eratio}");
        }
        println!("=========== RatedTicket error ratio end ===========");
    }

    fn consume(
        throughput: f64,
        iops: f64,
        bytes: &Arc<AtomicUsize>,
        ios: &Arc<AtomicUsize>,
        throttler: &Arc<IoThrottler>,
        target: Target,
    ) {
        let mut rng = rng();
        let t = match target {
            Target::Throughput => rng.random_range(throughput * 0.0008..throughput * 0.0016),
            Target::Iops => rng.random_range(throughput * 0.0004..throughput * 0.0008),
        };
        let i = match target {
            Target::Throughput => rng.random_range(iops * 0.0004..iops * 0.0008),
            Target::Iops => rng.random_range(iops * 0.0008..iops * 0.0016),
        };
        if throttler.consume(t, i) {
            bytes.fetch_add(t as usize, Ordering::Relaxed);
            ios.fetch_add(i as usize, Ordering::Relaxed);
        }
    }

    fn probe_reduce(
        throughput: f64,
        iops: f64,
        bytes: &Arc<AtomicUsize>,
        ios: &Arc<AtomicUsize>,
        throttler: &Arc<IoThrottler>,
        target: Target,
    ) {
        if throttler.probe() {
            let mut rng = rng();
            let t = match target {
                Target::Throughput => rng.random_range(throughput * 0.0008..throughput * 0.0016),
                Target::Iops => rng.random_range(throughput * 0.0004..throughput * 0.0008),
            };
            let i = match target {
                Target::Throughput => rng.random_range(iops * 0.0004..iops * 0.0008),
                Target::Iops => rng.random_range(iops * 0.0008..iops * 0.0016),
            };
            throttler.reduce(t, i);
            bytes.fetch_add(t as usize, Ordering::Relaxed);
            ios.fetch_add(i as usize, Ordering::Relaxed);
        }
    }

    fn case<F>(f: F, target: Target) -> f64
    where
        F: Fn(f64, f64, &Arc<AtomicUsize>, &Arc<AtomicUsize>, &Arc<IoThrottler>, Target) + Send + Sync + Copy + 'static,
    {
        const THREADS: usize = 8;
        const THROUGHPUT: f64 = (250 * 1024 * 1024) as f64;
        const IOPS: f64 = 100000.0;
        const DURATION: Duration = Duration::from_secs(10);

        let bytes = Arc::new(AtomicUsize::new(0));
        let ios = Arc::new(AtomicUsize::new(0));

        let throttler = Arc::new(IoThrottler::new(THROUGHPUT, IOPS));
        let task = |throughput: f64,
                    iops: f64,
                    bytes: Arc<AtomicUsize>,
                    ios: Arc<AtomicUsize>,
                    limiter: Arc<IoThrottler>,
                    f: F,
                    target: Target| {
            let start = Instant::now();
            let mut rng = rng();
            loop {
                if start.elapsed() >= DURATION {
                    break;
                }
                std::thread::sleep(Duration::from_millis(rng.random_range(1..10)));
                f(throughput, iops, &bytes, &ios, &limiter, target)
            }
        };
        let mut handles = vec![];
        for _ in 0..THREADS {
            let handle = std::thread::spawn({
                let bytes = bytes.clone();
                let ios = ios.clone();
                let throttler = throttler.clone();
                move || task(THROUGHPUT, IOPS, bytes, ios, throttler, f, target)
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let throughput_error =
            (bytes.load(Ordering::Relaxed) as isize - THROUGHPUT as isize * DURATION.as_secs() as isize).unsigned_abs();
        let throughput_error_ratio = throughput_error as f64 / (THROUGHPUT * DURATION.as_secs_f64());

        let iops_error =
            (ios.load(Ordering::Relaxed) as isize - IOPS as isize * DURATION.as_secs() as isize).unsigned_abs();
        let iops_error_ratio = iops_error as f64 / (IOPS * DURATION.as_secs_f64());

        match target {
            Target::Throughput => throughput_error_ratio,
            Target::Iops => iops_error_ratio,
        }
    }
}
