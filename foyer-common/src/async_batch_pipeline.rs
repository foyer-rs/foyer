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
use tokio::{runtime::Handle, task::JoinHandle};

#[derive(Debug)]
pub struct AsyncBatchPipeline<T, R> {
    inner: Arc<Mutex<AsyncBatchPipelineInner<T, R>>>,
    runtime: Handle,
}

impl<T, R> Clone for AsyncBatchPipeline<T, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            runtime: self.runtime.clone(),
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
        Self::with_runtime(state, Handle::current())
    }

    pub fn with_runtime(state: T, runtime: Handle) -> Self {
        Self {
            inner: Arc::new(Mutex::new(AsyncBatchPipelineInner {
                state,
                has_leader: false,
                handle: None,
            })),
            runtime,
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

    pub fn wait(&self) -> Option<JoinHandle<R>> {
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
    pub fn pipeline<FR, F, FU, NS>(mut self, new_state: NS, fr: FR, f: F) -> JoinHandle<()>
    where
        T: Send + 'static,
        R: Send + 'static,
        FR: FnOnce(R) + Send + 'static,
        F: FnOnce(T) -> FU + Send + 'static,
        FU: Future<Output = R> + Send + 'static,
        NS: FnOnce(&T) -> T + Send + 'static,
    {
        let handle = self.handle.take();
        let inner = self.batch.inner.clone();
        let runtime = self.batch.runtime.clone();

        self.batch.runtime.spawn(async move {
            if let Some(handle) = handle {
                fr(handle.await.unwrap());
            }

            let mut guard = inner.lock();
            let mut state = new_state(&guard.state);
            std::mem::swap(&mut guard.state, &mut state);
            let future = f(state);
            let handle = runtime.spawn(future);
            guard.handle = Some(handle);
            guard.has_leader = false;
        })
    }
}

#[cfg(test)]
mod tests {

    use futures::future::join_all;
    use itertools::Itertools;

    use super::*;

    #[tokio::test]
    async fn test_async_batch_pipeline() {
        let batch: AsyncBatchPipeline<Vec<u64>, Vec<u64>> = AsyncBatchPipeline::new(vec![]);
        let res = join_all((0..100).map(|i| {
            let batch = batch.clone();
            async move { batch.accumulate(|state| state.push(i)) }
        }))
        .await;

        let mut res = res.into_iter().flatten().collect_vec();
        assert_eq!(res.len(), 1);
        let token = res.remove(0);
        token
            .pipeline(|_| vec![], |_| unreachable!(), |state| async move { state })
            .await
            .unwrap();

        let res = join_all((100..200).map(|i| {
            let batch = batch.clone();
            async move { batch.accumulate(|state| state.push(i)) }
        }))
        .await;

        let mut res = res.into_iter().flatten().collect_vec();
        assert_eq!(res.len(), 1);
        let token = res.remove(0);
        token
            .pipeline(
                |_| vec![],
                |mut res| {
                    res.sort();
                    assert_eq!(res, (0..100).collect_vec());
                },
                |state| async move { state },
            )
            .await
            .unwrap();

        let mut res = batch.wait().unwrap().await.unwrap();
        res.sort();
        assert_eq!(res, (100..200).collect_vec());
    }
}
