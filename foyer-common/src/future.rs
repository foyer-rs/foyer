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
    pub store: Option<S>,
}

impl<T, S> From<T> for Diversion<T, S> {
    fn from(value: T) -> Self {
        Self {
            target: value,
            store: None,
        }
    }
}

/// [`DiversionFuture`] is a future wrapper that partially store and partially return the future result.
#[must_use]
#[pin_project]
pub struct DiversionFuture<FU, T, S> {
    #[pin]
    inner: FU,
    store: Option<S>,
    _marker: PhantomData<T>,
}

impl<FU, T, S> DiversionFuture<FU, T, S> {
    /// Create a new [`DiversionFuture`] wrapper.
    pub fn new(future: FU) -> Self {
        Self {
            inner: future,
            store: None,
            _marker: PhantomData,
        }
    }

    /// Get the stored state.
    pub fn store(&self) -> &Option<S> {
        &self.store
    }
}

impl<FU, T, S> Deref for DiversionFuture<FU, T, S> {
    type Target = FU;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<FU, T, S, I> Future for DiversionFuture<FU, T, S>
where
    FU: Future<Output = I>,
    I: Into<Diversion<T, S>>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let Diversion { target, store } = ready!(this.inner.poll(cx)).into();
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
        let mut f = pin!(DiversionFuture::new(async move {
            Diversion {
                target: "The answer to life, the universe, and everything.".to_string(),
                store: Some(42),
            }
        },));

        let question: String = poll_fn(|cx| f.as_mut().poll(cx)).await;
        let answer = f.store().unwrap();

        assert_eq!(
            (question.as_str(), answer),
            ("The answer to life, the universe, and everything.", 42)
        );
    }
}
