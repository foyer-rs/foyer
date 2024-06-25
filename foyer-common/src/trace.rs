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

use minitrace::prelude::*;

use std::{
    ops::Deref,
    pin::Pin,
    sync::atomic::AtomicUsize,
    task::{Context, Poll},
    time::Duration,
};

use futures::{ready, Future};
use pin_project::pin_project;

/// Configurations for trace.
#[derive(Debug)]
pub struct TraceConfig {
    /// Threshold for recording the hybrid cache `insert` and `insert_with_context` operation in us.
    pub record_hybrid_insert_threshold_us: AtomicUsize,
    /// Threshold for recording the hybrid cache `get` operation in us.
    pub record_hybrid_get_threshold_us: AtomicUsize,
    /// Threshold for recording the hybrid cache `obtain` operation in us.
    pub record_hybrid_obtain_threshold_us: AtomicUsize,
    /// Threshold for recording the hybrid cache `remove` operation in us.
    pub record_hybrid_remove_threshold_us: AtomicUsize,
    /// Threshold for recording the hybrid cache `fetch` operation in us.
    pub record_hybrid_fetch_threshold_us: AtomicUsize,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            record_hybrid_insert_threshold_us: AtomicUsize::from(1000 * 1000),
            record_hybrid_get_threshold_us: AtomicUsize::from(1000 * 1000),
            record_hybrid_obtain_threshold_us: AtomicUsize::from(1000 * 1000),
            record_hybrid_remove_threshold_us: AtomicUsize::from(1000 * 1000),
            record_hybrid_fetch_threshold_us: AtomicUsize::from(1000 * 1000),
        }
    }
}

/// [`InRootSpan`] provides similar features like [`minitrace::future::InSpan`] with more controls.
#[pin_project]
pub struct InRootSpan<F> {
    #[pin]
    inner: F,

    root: Option<Span>,
    threshold: Option<Duration>,
}

impl<F> InRootSpan<F> {
    /// Create a traced future with the given root span.
    pub fn new(inner: F, root: Span) -> Self
    where
        F: Future,
    {
        Self {
            inner,
            root: Some(root),
            threshold: None,
        }
    }

    /// Set record threshold for the root span.
    ///
    /// If the duration of the root span is lower than the threshold, the root span will not be recorded.
    pub fn with_threshold(mut self, threshold: Duration) -> Self {
        self.threshold = Some(threshold);
        self
    }
}

impl<F> Future for InRootSpan<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let _guard = this.root.as_ref().map(|s| s.set_local_parent());
        let res = ready!(this.inner.poll(cx));

        let mut root = this.root.take().unwrap();

        if let (Some(elapsed), Some(threshold)) = (root.elapsed(), this.threshold.as_ref()) {
            if &elapsed < threshold {
                root.cancel();
            }
        }

        Poll::Ready(res)
    }
}

impl<F> Deref for InRootSpan<F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
