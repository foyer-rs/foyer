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

use pin_project::pin_project;
use std::{
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    task::{ready, Context, Poll},
};

/// Result that the inner future of a [`DiversionFuture`] should return.
///
/// - The `target` will be further returned by [`DiversionFuture`].
/// - The `store` will be stored in the [`DiversionFuture`].
pub struct Diversion<T, S> {
    /// The `target` will be further returned by [`DiversionFuture`].
    pub target: T,
    /// The `store` will be stored in the [`DiversionFuture`].
    pub store: S,
}

impl<T, S> From<T> for Diversion<T, S>
where
    S: Default,
{
    fn from(value: T) -> Self {
        Self {
            target: value,
            store: S::default(),
        }
    }
}

/// [`DiversionFuture`] is a future wrapper that partially store and partially return the future result.
#[pin_project]
pub struct DiversionFuture<FU, T, S> {
    #[pin]
    inner: FU,
    store: S,
    _marker: PhantomData<T>,
}

impl<FU, T, S> DiversionFuture<FU, T, S> {
    /// Create a new [`DiversionFuture`] wrapper.
    pub fn new(future: FU, init: S) -> Self {
        Self {
            inner: future,
            store: init,
            _marker: PhantomData,
        }
    }

    /// Create a new [`DiversionFuture`] wrapper with default store value.
    pub fn with_default(future: FU) -> Self
    where
        S: Default,
    {
        Self {
            inner: future,
            store: S::default(),
            _marker: PhantomData,
        }
    }

    /// Get the stored state.
    pub fn store(&self) -> &S {
        &self.store
    }
}

impl<FU, T, S> Deref for DiversionFuture<FU, T, S> {
    type Target = FU;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<FU, T, S> Future for DiversionFuture<FU, T, S>
where
    FU: Future<Output = Diversion<T, S>>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let Diversion { target, store } = ready!(this.inner.poll(cx));
        *this.store = store;
        Poll::Ready(target)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::future::poll_fn;

    use super::*;

    #[tokio::test]
    async fn test_diversion_future() {
        let mut f = pin!(DiversionFuture::new(
            async move {
                Diversion {
                    target: "The answer to life, the universe, and everything.".to_string(),
                    store: 42,
                }
            },
            0,
        ));

        let question = poll_fn(|cx| f.as_mut().poll(cx)).await;
        let answer = *f.store();

        assert_eq!(
            (question.as_str(), answer),
            ("The answer to life, the universe, and everything.", 42)
        );
    }
}
