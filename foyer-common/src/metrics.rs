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

use std::fmt::Debug;

use metrics::{counter, gauge, Counter, Gauge};

#[derive(Clone)]
pub struct Metrics {
    pub memory_insert: Counter,
    pub memory_replace: Counter,
    pub memory_hit: Counter,
    pub memory_miss: Counter,
    pub memory_remove: Counter,
    pub memory_evict: Counter,
    pub memory_reinsert: Counter,
    pub memory_release: Counter,
    pub memory_queue: Counter,
    pub memory_fetch: Counter,

    pub memory_usage: Gauge,
    // pub insert: Counter,
    // pub insert_duration: Histogram,

    // pub lookup_hit: Counter,
    // pub lookup_hit_duration: Histogram,
    // pub lookup_miss: Counter,
    // pub lookup_miss_duration: Histogram,
}

impl Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish()
    }
}

impl Metrics {
    pub fn new(name: &str) -> Self {
        let memory_insert = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "insert");
        let memory_replace = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "replace");
        let memory_hit = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "hit");
        let memory_miss = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "miss");
        let memory_remove = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "remove");
        let memory_evict = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "evict");
        let memory_reinsert =
            counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "reinsert");
        let memory_release = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "release");
        let memory_queue = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "queue");
        let memory_fetch = counter!(format!("foyer_memory_op_total"), "name" => name.to_string(), "op" => "fetch");

        let memory_usage = gauge!(format!("foyer_memory_usage"), "name" => name.to_string(), "op" => "usage");

        Self {
            memory_insert,
            memory_replace,
            memory_hit,
            memory_miss,
            memory_remove,
            memory_evict,
            memory_reinsert,
            memory_release,
            memory_queue,
            memory_fetch,
            memory_usage,
        }
        // let insert = counter!(format!("{name}_op_total"), "op" => "insert");
        // let insert_duration = histogram!(format!("{name}_op_duration"), "op" => "insert");

        // let lookup_hit = counter!(format!("{name}_op_total"), "op" => "lookup", "extra" => "hit");
        // let lookup_hit_duration = histogram!(format!("{name}_op_duration"), "op" => "lookup", "extra" => "hit");
        // let lookup_miss = counter!(format!("{name}_op_total"), "op" => "lookup", "extra" => "miss");
        // let lookup_miss_duration = histogram!(format!("{name}_op_duration"), "op" => "lookup", "extra" => "miss");

        // Self {
        //     insert,
        //     insert_duration,
        //     lookup_hit,
        //     lookup_hit_duration,
        //     lookup_miss,
        //     lookup_miss_duration,
        // }
    }
}
