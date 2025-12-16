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

use std::{fmt::Display, num::NonZeroUsize, str::FromStr};

/// Device iops counter.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IopsCounter {
    /// Count 1 iops for each read/write.
    #[default]
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
        let s = s.trim();
        match s {
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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

impl Throttle {
    /// Create a new unlimited throttle config.
    pub fn new() -> Self {
        Self::default()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(all(miri, not(target_os = "linux")), ignore = "requires Linux for tokio+miri")]
    fn test_throttle_default() {
        assert!(matches!(
            Throttle::new(),
            Throttle {
                write_iops: None,
                read_iops: None,
                write_throughput: None,
                read_throughput: None,
                iops_counter: IopsCounter::PerIo,
            }
        ));
    }

    #[test]
    #[cfg_attr(all(miri, not(target_os = "linux")), ignore = "requires Linux for tokio+miri")]
    fn test_iops_counter_from_str() {
        assert!(matches!(IopsCounter::from_str("PerIo"), Ok(IopsCounter::PerIo)));
        assert!(matches!(IopsCounter::from_str(" PerIo "), Ok(IopsCounter::PerIo)));
        assert!(matches!(IopsCounter::from_str("PerIo "), Ok(IopsCounter::PerIo)));
        assert!(matches!(IopsCounter::from_str(" PerIo"), Ok(IopsCounter::PerIo)));

        let _num = NonZeroUsize::new(1024).unwrap();

        assert!(matches!(
            IopsCounter::from_str("PerIoSize(1024)"),
            Ok(IopsCounter::PerIoSize(_num))
        ));
        assert!(matches!(
            IopsCounter::from_str(" PerIoSize(1024) "),
            Ok(IopsCounter::PerIoSize(_num))
        ));
        assert!(matches!(
            IopsCounter::from_str("PerIoSize(1024) "),
            Ok(IopsCounter::PerIoSize(_num))
        ));
        assert!(matches!(
            IopsCounter::from_str(" PerIoSize(1024)"),
            Ok(IopsCounter::PerIoSize(_num))
        ));

        assert!(IopsCounter::from_str("PerIoSize(0)").is_err());
        assert!(IopsCounter::from_str("PerIoSize(1024a)").is_err());

        assert!(IopsCounter::from_str("invalid_string").is_err());
    }
}
