// Copyright 2025 foyer Project Authors
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

/// Errors enum for foyer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// foyer in-memory cache error.
    #[error("foyer memory error: {0}")]
    Memory(#[from] foyer_memory::Error),
    /// foyer disk cache error.
    #[error("foyer storage error: {0}")]
    Storage(#[from] foyer_storage::Error),
    #[error("other error: {0}")]
    /// Other error.
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Error {
    /// Create customized error.
    pub fn other<E>(e: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::Other(e.into())
    }

    /// Downcast error to a specific type.
    ///
    /// Only `Other` variant can be downcasted.
    /// If the error is not `Other`, it will return an error indicating that downcasting is not possible.
    pub fn downcast<T>(self) -> std::result::Result<T, Self>
    where
        T: std::error::Error + 'static,
    {
        match self {
            Self::Other(e) => e.downcast::<T>().map(|e| *e).map_err(|e| {
                let e: Box<dyn std::error::Error + Send + Sync + 'static> =
                    format!("cannot downcast error: {e}").into();
                Self::Other(e)
            }),
            _ => {
                let e: Box<dyn std::error::Error + Send + Sync + 'static> = "only other error can be downcast".into();
                Err(e.into())
            }
        }
    }
}

/// Result type for foyer.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_other_error_downcast() {
        #[derive(Debug, PartialEq, Eq)]
        struct TestError(String);

        impl std::fmt::Display for TestError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "TestError: {}", self.0)
            }
        }

        impl std::error::Error for TestError {}

        let e = Error::Other(Box::new(TestError(
            "Error or not error, that is a question.".to_string(),
        )));

        let res = match e {
            Error::Memory(_) | Error::Storage(_) => unreachable!(),
            Error::Other(e) => e.downcast::<TestError>(),
        }
        .unwrap();
        assert_eq!(
            res,
            Box::new(TestError("Error or not error, that is a question.".to_string(),))
        );

        let e = Error::Other(Box::new(TestError(
            "Error or not error, that is a question.".to_string(),
        )));
        let res = e.downcast::<TestError>().unwrap();
        assert_eq!(res, TestError("Error or not error, that is a question.".to_string()));
    }
}
