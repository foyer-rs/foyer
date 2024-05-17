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

use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

#[derive(Clone)]
pub struct Metrics {
    /* in-memory cache metrics */
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

    /* disk cache metrics */
    pub storage_enqueue: Counter,
    pub storage_hit: Counter,
    pub storage_miss: Counter,
    pub storage_delete: Counter,

    pub storage_enqueue_duration: Histogram,
    pub storage_hit_duration: Histogram,
    pub storage_miss_duration: Histogram,
    pub storage_delete_duration: Histogram,

    pub storage_queue_rotate: Counter,
    pub storage_queue_leader: Counter,
    pub storage_queue_follower: Counter,
    pub storage_queue_rotate_duration: Histogram,

    pub storage_disk_write: Counter,
    pub storage_disk_read: Counter,
    pub storage_disk_flush: Counter,

    pub storage_disk_write_bytes: Counter,
    pub storage_disk_read_bytes: Counter,

    pub storage_disk_write_duration: Histogram,
    pub storage_disk_read_duration: Histogram,
    pub storage_disk_flush_duration: Histogram,
    /* hybrid cache metrics */
    // TODO(MrCroxx): Add hybrid cache mtrics.
}

impl Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish()
    }
}

impl Metrics {
    pub fn new(name: &str) -> Self {
        /* in-memory cache metrics */

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

        /* disk cache metrics */

        let storage_enqueue =
            counter!(format!("foyer_storage_op_total"), "name" => name.to_string(), "op" => "enqueue");
        let storage_hit = counter!(format!("foyer_storage_op_total"), "name" => name.to_string(), "op" => "hit");
        let storage_miss = counter!(format!("foyer_storage_op_total"), "name" => name.to_string(), "op" => "miss");
        let storage_delete = counter!(format!("foyer_storage_op_total"), "name" => name.to_string(), "op" => "delete");

        let storage_enqueue_duration =
            histogram!(format!("foyer_storage_op_duration"), "name" => name.to_string(), "op" => "enqueue");
        let storage_hit_duration =
            histogram!(format!("foyer_storage_op_duration"), "name" => name.to_string(), "op" => "hit");
        let storage_miss_duration =
            histogram!(format!("foyer_storage_op_duration"), "name" => name.to_string(), "op" => "miss");
        let storage_delete_duration =
            histogram!(format!("foyer_storage_op_duration"), "name" => name.to_string(), "op" => "delete");

        let storage_queue_rotate =
            counter!(format!("foyer_storage_inner_op_total"), "name" => name.to_string(), "op" => "queue_rotate");
        let storage_queue_leader =
            counter!(format!("foyer_storage_inner_op_total"), "name" => name.to_string(), "op" => "queue_leader");
        let storage_queue_follower =
            counter!(format!("foyer_storage_inner_op_total"), "name" => name.to_string(), "op" => "queue_follower");
        let storage_queue_rotate_duration =
            histogram!(format!("foyer_storage_inner_op_duration"), "name" => name.to_string(), "op" => "queue_rotate");

        let storage_disk_write =
            counter!(format!("foyer_storage_disk_io_total"), "name" => name.to_string(), "op" => "write");
        let storage_disk_read =
            counter!(format!("foyer_storage_disk_io_total"), "name" => name.to_string(), "op" => "read");
        let storage_disk_flush =
            counter!(format!("foyer_storage_disk_io_total"), "name" => name.to_string(), "op" => "flush");

        let storage_disk_write_bytes =
            counter!(format!("foyer_storage_disk_io_bytes"), "name" => name.to_string(), "op" => "write");
        let storage_disk_read_bytes =
            counter!(format!("foyer_storage_disk_io_bytes"), "name" => name.to_string(), "op" => "read");

        let storage_disk_write_duration =
            histogram!(format!("foyer_storage_disk_io_duration"), "name" => name.to_string(), "op" => "write");
        let storage_disk_read_duration =
            histogram!(format!("foyer_storage_disk_io_duration"), "name" => name.to_string(), "op" => "read");
        let storage_disk_flush_duration =
            histogram!(format!("foyer_storage_disk_io_duration"), "name" => name.to_string(), "op" => "flush");

        /* hybrid cache metrics */

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

            storage_enqueue,
            storage_hit,
            storage_miss,
            storage_delete,
            storage_enqueue_duration,
            storage_hit_duration,
            storage_miss_duration,
            storage_delete_duration,
            storage_queue_rotate,
            storage_queue_leader,
            storage_queue_follower,
            storage_queue_rotate_duration,
            storage_disk_write,
            storage_disk_read,
            storage_disk_flush,
            storage_disk_write_bytes,
            storage_disk_read_bytes,
            storage_disk_write_duration,
            storage_disk_read_duration,
            storage_disk_flush_duration,
        }
    }
}
