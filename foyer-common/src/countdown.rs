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

use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

#[derive(Debug)]
pub struct Countdown {
    finish: AtomicBool,
    counter: AtomicIsize,
}

impl Countdown {
    /// Countdown `counter` times.
    ///
    /// # Safety
    ///
    /// Panics if `counter` exceeds [`isize::MAX`].
    pub fn new(counter: usize) -> Self {
        Self {
            finish: AtomicBool::new(false),
            counter: AtomicIsize::new(isize::try_from(counter).expect("`counter` must NOT exceed `isize::MAX`.")),
        }
    }

    /// Returns `false` for the first `counter` times, then always returns `true`.
    pub fn countdown(&self) -> bool {
        if self.finish.load(Ordering::Relaxed) {
            return true;
        }
        self.counter.fetch_sub(1, Ordering::Relaxed) <= 0
    }

    /// Reset [`Countdown`] with `counter`.
    pub fn reset(&self, counter: usize) {
        self.finish.store(false, Ordering::Relaxed);
        self.counter.store(
            isize::try_from(counter).expect("`counter` must NOT exceed `isize::MAX`."),
            Ordering::Relaxed,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future::join_all;

    use super::*;

    async fn case(counter: usize, concurrency: usize) {
        let cd = Countdown::new(counter);
        let res = join_all((0..concurrency).map(|_| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cd.countdown()
        }))
        .await;
        assert_eq!(counter, res.into_iter().filter(|b| !b).count());
    }

    #[tokio::test]
    async fn test_countdown() {
        for counter in [1, 4, 8, 16] {
            for concurrency in [16, 32, 64, 128] {
                case(counter, concurrency).await;
            }
        }
    }
}
