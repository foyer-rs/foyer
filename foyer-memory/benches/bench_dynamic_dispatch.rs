// Copyright 2026 foyer Project Authors
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

//! micro benchmark for dynamic dispatch

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rand::{distr::Alphanumeric, rng, Rng};

struct T<F>
where
    F: Fn(&str) -> usize,
{
    f1: F,
    f2: Box<dyn Fn(&str) -> usize>,
    f3: Arc<dyn Fn(&str) -> usize>,
}

fn rand_string(len: usize) -> String {
    rng().sample_iter(&Alphanumeric).take(len).map(char::from).collect()
}

fn bench_static_dispatch<F>(t: &T<F>, loops: usize) -> Duration
where
    F: Fn(&str) -> usize,
{
    let mut dur = Duration::default();
    for _ in 0..loops {
        let s = rand_string(rng().random_range(0..100));
        let now = Instant::now();
        let _ = (t.f1)(&s);
        dur += now.elapsed();
    }
    Duration::from_nanos((dur.as_nanos() as usize / loops) as _)
}

fn bench_box_dynamic_dispatch<F>(t: &T<F>, loops: usize) -> Duration
where
    F: Fn(&str) -> usize,
{
    let mut dur = Duration::default();
    for _ in 0..loops {
        let s = rand_string(rng().random_range(0..100));
        let now = Instant::now();
        let _ = (t.f3)(&s);
        dur += now.elapsed();
    }
    Duration::from_nanos((dur.as_nanos() as usize / loops) as _)
}

fn bench_arc_dynamic_dispatch<F>(t: &T<F>, loops: usize) -> Duration
where
    F: Fn(&str) -> usize,
{
    let mut dur = Duration::default();
    for _ in 0..loops {
        let s = rand_string(rng().random_range(0..100));
        let now = Instant::now();
        let _ = (t.f2)(&s);
        dur += now.elapsed();
    }
    Duration::from_nanos((dur.as_nanos() as usize / loops) as _)
}

fn main() {
    let t = T {
        f1: |s: &str| s.len(),
        f2: Box::new(|s: &str| s.len()),
        f3: Arc::new(|s: &str| s.len()),
    };

    let _ = T {
        f1: |s: &str| s.len(),
        f2: Box::new(|s: &str| s.len() + 1),
        f3: Arc::new(|s: &str| s.len() + 1),
    };

    let _ = T {
        f1: |s: &str| s.len(),
        f2: Box::new(|s: &str| s.len() + 2),
        f3: Arc::new(|s: &str| s.len() + 2),
    };

    let _ = T {
        f1: |s: &str| s.len(),
        f2: Box::new(|s: &str| s.len() + 3),
        f3: Arc::new(|s: &str| s.len() + 3),
    };

    for loops in [100_000, 1_000_000, 10_000_000] {
        println!();

        println!("     static - {} loops : {:?}", loops, bench_static_dispatch(&t, loops));
        println!(
            "box dynamic - {} loops : {:?}",
            loops,
            bench_box_dynamic_dispatch(&t, loops)
        );
        println!(
            "arc dynamic - {} loops : {:?}",
            loops,
            bench_arc_dynamic_dispatch(&t, loops)
        );
    }
}
