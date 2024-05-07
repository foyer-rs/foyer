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

use std::{future::Future, sync::Arc};

use parking_lot::Mutex;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct AsyncBatchPipeline<T, R> {
    inner: Arc<Mutex<AsyncBatchPipelineInner<T, R>>>,
}

impl<T, R> Clone for AsyncBatchPipeline<T, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug)]
struct AsyncBatchPipelineInner<T, R> {
    state: T,
    has_leader: bool,
    handle: Option<JoinHandle<R>>,
}

pub struct LeaderToken<T, R> {
    batch: AsyncBatchPipeline<T, R>,
    handle: Option<JoinHandle<R>>,
}

impl<T, R> AsyncBatchPipeline<T, R> {
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(AsyncBatchPipelineInner {
                state,
                has_leader: false,
                handle: None,
            })),
        }
    }

    pub fn accumulate<F>(&self, f: F) -> Option<LeaderToken<T, R>>
    where
        F: FnOnce(&mut T),
    {
        let mut inner = self.inner.lock();

        let token = if !inner.has_leader {
            inner.has_leader = true;
            Some(LeaderToken {
                batch: self.clone(),
                handle: inner.handle.take(),
            })
        } else {
            None
        };

        f(&mut inner.state);

        token
    }

    pub fn close(&self) -> Option<JoinHandle<R>> {
        self.inner.lock().handle.take()
    }
}

impl<T, R> LeaderToken<T, R> {
    /// Pipeline execute futures.
    ///
    /// `new_state`
    /// - Receives the reference of the old state and returns the new state.
    ///
    /// `f`
    /// - Receives the owned old state and returns a future.
    /// - The future will be polled after handling the previous result.
    /// - The future is guaranteed to be execute one by one in order.
    ///
    /// `fr`
    /// - Handle the previous result.
    pub fn pipeline<FR, F, FU, NS>(&mut self, new_state: NS, fr: FR, f: F)
    where
        R: Send + 'static,
        FR: FnOnce(R) + Send + 'static,
        F: FnOnce(T) -> FU,
        FU: Future<Output = R> + Send + 'static,
        NS: FnOnce(&T) -> T,
    {
        let handle = self.handle.take();

        let mut inner = self.batch.inner.lock();

        let mut state = new_state(&inner.state);
        std::mem::swap(&mut inner.state, &mut state);

        let future = f(state);
        let handle = tokio::spawn(async move {
            if let Some(handle) = handle {
                fr(handle.await.unwrap());
            }
            future.await
        });

        inner.handle = Some(handle);
        inner.has_leader = false;
    }
}
