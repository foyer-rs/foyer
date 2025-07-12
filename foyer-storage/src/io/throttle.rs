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
    fmt::Display,
    num::NonZeroUsize,
    str::FromStr,
    time::{Duration, Instant},
};

use parking_lot::Mutex;

/// Device iops counter.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IopsCounter {
    /// Count 1 iops for each read/write.
    PerIo,
    /// Count 1 iops for each read/write with the size of the i/o.
    PerIoSize(NonZeroUsize),
}

impl IopsCounter {
    /// Create a new iops counter that count 1 iops for each io.
    pub fn per_io() -> Self {
        Self::PerIo
    }

    /// Create a new iops counter that count 1 iops for every io size in bytes among ios.
    ///
    /// NOTE: `io_size` must NOT be zero.
    pub fn per_io_size(io_size: usize) -> Self {
        Self::PerIoSize(NonZeroUsize::new(io_size).expect("io size must be non-zero"))
    }

    /// Count io(s) by io size in bytes.
    pub fn count(&self, bytes: usize) -> usize {
        match self {
            IopsCounter::PerIo => 1,
            IopsCounter::PerIoSize(size) => bytes / *size + if bytes % *size != 0 { 1 } else { 0 },
        }
    }
}

impl Display for IopsCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IopsCounter::PerIo => write!(f, "PerIo"),
            IopsCounter::PerIoSize(size) => write!(f, "PerIoSize({size})"),
        }
    }
}

impl FromStr for IopsCounter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim() {
            "PerIo" => Ok(IopsCounter::PerIo),
            _ if s.starts_with("PerIoSize(") && s.ends_with(')') => {
                let num = &s[10..s.len() - 1];
                let v = num.parse::<NonZeroUsize>()?;
                Ok(IopsCounter::PerIoSize(v))
            }
            _ => Err(anyhow::anyhow!("Invalid IopsCounter format: {}", s)),
        }
    }
}

/// Throttle config for the device.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "clap", derive(clap::Args))]
pub struct Throttle {
    /// The maximum write iops for the device.
    #[cfg_attr(feature = "clap", clap(long))]
    pub write_iops: Option<NonZeroUsize>,
    /// The maximum read iops for the device.
    #[cfg_attr(feature = "clap", clap(long))]
    pub read_iops: Option<NonZeroUsize>,
    /// The maximum write throughput for the device.
    #[cfg_attr(feature = "clap", clap(long))]
    pub write_throughput: Option<NonZeroUsize>,
    /// The maximum read throughput for the device.
    #[cfg_attr(feature = "clap", clap(long))]
    pub read_throughput: Option<NonZeroUsize>,
    /// The iops counter for the device.
    #[cfg_attr(feature = "clap", clap(long, default_value = "PerIo"))]
    pub iops_counter: IopsCounter,
}

impl Default for Throttle {
    fn default() -> Self {
        Self::new()
    }
}

impl Throttle {
    /// Create a new unlimited throttle config.
    pub fn new() -> Self {
        Self {
            write_iops: None,
            read_iops: None,
            write_throughput: None,
            read_throughput: None,
            iops_counter: IopsCounter::PerIo,
        }
    }

    /// Set the maximum write iops for the device.
    pub fn with_write_iops(mut self, iops: usize) -> Self {
        self.write_iops = NonZeroUsize::new(iops);
        self
    }

    /// Set the maximum read iops for the device.
    pub fn with_read_iops(mut self, iops: usize) -> Self {
        self.read_iops = NonZeroUsize::new(iops);
        self
    }

    /// Set the maximum write throughput for the device.
    pub fn with_write_throughput(mut self, throughput: usize) -> Self {
        self.write_throughput = NonZeroUsize::new(throughput);
        self
    }

    /// Set the maximum read throughput for the device.
    pub fn with_read_throughput(mut self, throughput: usize) -> Self {
        self.read_throughput = NonZeroUsize::new(throughput);
        self
    }

    /// Set the iops counter for the device.
    pub fn with_iops_counter(mut self, counter: IopsCounter) -> Self {
        self.iops_counter = counter;
        self
    }
}

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
