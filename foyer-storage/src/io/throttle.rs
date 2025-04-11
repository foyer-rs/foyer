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

use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

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
    /// Note: `None` stands for umlimited.
    pub fn new(throughput: Option<NonZeroUsize>, iops: Option<NonZeroUsize>) -> Self {
        let inner = Inner {
            throughput_quota: 0.0,
            iops_quota: 0.0,
            last: Instant::now(),
        };
        let throughput = throughput.map(|v| v.get() as f64).unwrap_or_default();
        let iops = iops.map(|v| v.get() as f64).unwrap_or_default();
        Self {
            inner: Mutex::new(inner),
            throughput,
            iops,
        }
    }

    /// Get the nearest time to retry to check if an io can be submitted.
    ///
    /// Return `Duration::ZERO` if no need to wait.
    pub fn probe(&self) -> Duration {
        let mut inner = self.inner.lock();

        let now = Instant::now();
        let dur = now.duration_since(inner.last).as_secs_f64();
        let throughput_refill = dur * self.throughput;
        let iops_refill = dur * self.iops;
        inner.last = now;
        inner.throughput_quota = f64::min(inner.throughput_quota + throughput_refill, self.throughput);
        inner.iops_quota = f64::min(inner.iops_quota + iops_refill, self.iops);

        let throughput_refill_duration = if self.throughput == 0.0 || inner.throughput_quota >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-inner.throughput_quota / self.throughput)
        };
        let iops_refill_duration = if self.iops == 0.0 || inner.iops_quota >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-inner.iops_quota / self.iops)
        };
        std::cmp::max(throughput_refill_duration, iops_refill_duration)
    }

    /// Submit io that consumes the given throughput and iops qutoa.
    pub fn reduce(&self, throughput: f64, iops: f64) {
        let mut inner = self.inner.lock();
        inner.throughput_quota -= throughput;
        inner.iops_quota -= iops;
    }

    /// Get the nearest time to retry to check if an io can be submitted.
    ///
    /// Return `Duration::ZERO` if no need to wait and consume the quota.
    pub fn consume(&self, throughput: f64, iops: f64) -> Duration {
        let mut inner = self.inner.lock();

        let now = Instant::now();
        let dur = now.duration_since(inner.last).as_secs_f64();
        let throughput_refill = dur * self.throughput;
        let iops_refill = dur * self.iops;
        inner.last = now;
        inner.throughput_quota = f64::min(inner.throughput_quota + throughput_refill, self.throughput);
        inner.iops_quota = f64::min(inner.iops_quota + iops_refill, self.iops);

        let throughput_refill_duration = if self.throughput == 0.0 || inner.throughput_quota >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-inner.throughput_quota / self.throughput)
        };
        let iops_refill_duration = if self.iops == 0.0 || inner.iops_quota >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-inner.iops_quota / self.iops)
        };
        let wait = std::cmp::max(throughput_refill_duration, iops_refill_duration);

        if wait.is_zero() {
            if self.throughput > 0.0 {
                inner.throughput_quota -= throughput;
            }
            if self.iops > 0.0 {
                inner.iops_quota -= iops;
            }
        }

        wait
    }

    /// Get the throughput throttle.
    pub fn throughput(&self) -> f64 {
        self.throughput
    }

    /// Get the iops throttle.
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
    fn test_io_throttler_consume_throughput() {
        test(consume, Target::Throughput)
    }

    #[ignore]
    #[test]
    fn test_io_throttler_consume_iops() {
        test(consume, Target::Iops)
    }

    #[ignore]
    #[test]
    fn test_io_throttler_probe_reduce_throughput() {
        test(probe_reduce, Target::Throughput)
    }

    #[ignore]
    #[test]
    fn test_io_throttler_probe_reduce_iops() {
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
            assert!(
                eratio.abs() < ERATIO,
                "eratio: {}, eratio (abs): {} < ERATIO: {}",
                eratio,
                eratio.abs(),
                ERATIO
            );
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
        if throttler.consume(t, i).is_zero() {
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
        if throttler.probe().is_zero() {
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
        const THROUGHPUT: usize = 250 * 1024 * 1024;
        const IOPS: usize = 100000;
        const DURATION: Duration = Duration::from_secs(10);

        let bytes = Arc::new(AtomicUsize::new(0));
        let ios = Arc::new(AtomicUsize::new(0));

        let throttler = Arc::new(IoThrottler::new(NonZeroUsize::new(THROUGHPUT), NonZeroUsize::new(IOPS)));
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
                std::thread::sleep(Duration::from_micros(rng.random_range(10..100)));
                f(throughput, iops, &bytes, &ios, &limiter, target)
            }
        };
        let mut handles = vec![];
        for _ in 0..THREADS {
            let handle = std::thread::spawn({
                let bytes = bytes.clone();
                let ios = ios.clone();
                let throttler = throttler.clone();
                move || task(THROUGHPUT as _, IOPS as _, bytes, ios, throttler, f, target)
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let throughput_error =
            (bytes.load(Ordering::Relaxed) as isize - THROUGHPUT as isize * DURATION.as_secs() as isize).abs();
        let throughput_error_ratio = throughput_error as f64 / (THROUGHPUT as f64 * DURATION.as_secs_f64());

        let iops_error = (ios.load(Ordering::Relaxed) as isize - IOPS as isize * DURATION.as_secs() as isize).abs();
        let iops_error_ratio = iops_error as f64 / (IOPS as f64 * DURATION.as_secs_f64());

        match target {
            Target::Throughput => throughput_error_ratio,
            Target::Iops => iops_error_ratio,
        }
    }
}
