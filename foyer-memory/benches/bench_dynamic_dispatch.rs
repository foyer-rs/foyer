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

use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

pub trait Processor {
    fn process(&self) -> usize;
}

pub struct SimpleProcessor {
    data: usize,
}

impl SimpleProcessor {
    pub fn new(data: usize) -> Self {
        SimpleProcessor { data }
    }
}

impl Processor for SimpleProcessor {
    fn process(&self) -> usize {
        self.data * self.data
    }
}

pub struct ComplexProcessor {
    data: Vec<usize>,
}

impl ComplexProcessor {
    pub fn new(data: Vec<usize>) -> Self {
        ComplexProcessor { data }
    }
}

impl Processor for ComplexProcessor {
    fn process(&self) -> usize {
        self.data.iter().sum()
    }
}

fn bench_processor_types(c: &mut Criterion) {
    let simple_data = 42;
    let complex_data: Vec<usize> = (1..=10_000).collect();

    let simple_processor = SimpleProcessor::new(simple_data);
    let box_simple_processor: Box<dyn Processor> = Box::new(SimpleProcessor::new(simple_data));
    let arc_simple_processor: Arc<dyn Processor> = Arc::new(SimpleProcessor::new(simple_data));

    let complex_processor = ComplexProcessor::new(complex_data.clone());
    let box_complex_processor: Box<dyn Processor> = Box::new(ComplexProcessor::new(complex_data.clone()));
    let arc_complex_processor: Arc<dyn Processor> = Arc::new(ComplexProcessor::new(complex_data.clone()));

    let mut group = c.benchmark_group("Processor Types");
    group.bench_function("simple_direct_reference", |b| b.iter(|| simple_processor.process()));
    group.bench_function("simple_box_dynamic_dispatch", |b| {
        b.iter(|| box_simple_processor.process())
    });
    group.bench_function("simple_arc_dynamic_dispatch", |b| {
        b.iter(|| arc_simple_processor.process())
    });

    group.bench_function("complex_direct_reference", |b| b.iter(|| complex_processor.process()));
    group.bench_function("complex_box_dynamic_dispatch", |b| {
        b.iter(|| box_complex_processor.process())
    });
    group.bench_function("complex_arc_dynamic_dispatch", |b| {
        b.iter(|| arc_complex_processor.process())
    });

    group.finish();
}

/*
Processor Types/simple_direct_reference time:   [299.32 ps 299.45 ps 299.67 ps]
simple_box_dynamic_dispatch time:   [1.4963 ns 1.4969 ns 1.4980 ns]
simple_arc_dynamic_dispatch time:   [1.4962 ns 1.4964 ns 1.4967 ns]

complex_direct_reference time:   [753.43 ns 753.53 ns 753.65 ns]
complex_box_dynamic_dispatch time:   [757.12 ns 757.31 ns 757.58 ns]
complex_arc_dynamic_dispatch time:   [757.55 ns 757.67 ns 757.84 ns]
*/
criterion_group!(benches, bench_processor_types);
criterion_main!(benches);
