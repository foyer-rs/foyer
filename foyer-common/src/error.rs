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

use std::{
    backtrace::Backtrace,
    fmt::{Debug, Display},
    sync::Arc,
};

/// ErrorKind is all kinds of Error of foyer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// I/O error.
    Io,
    /// External error.
    External,
    /// Config error.
    Config,
    /// Channel closed.
    ChannelClosed,
    /// Task cancelled.
    TaskCancelled,
    /// Join error.
    Join,
    /// Parse error.
    Parse,
    /// Buffer size limit.
    ///
    /// Not a real error.
    ///
    /// Indicates that the buffer size has exceeded the limit and the caller may allocate a larger buffer and retry.
    BufferSizeLimit,
    /// Checksum mismatch.
    ChecksumMismatch,
    /// Magic mismatch.
    MagicMismatch,
    /// Out of range.
    OutOfRange,
    /// No sapce.
    NoSpace,
    /// Closed.
    Closed,
    /// Recover error.
    Recover,
    /// Unsupported operation.
    Unsupported,
}

impl ErrorKind {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<ErrorKind> for &'static str {
    fn from(v: ErrorKind) -> &'static str {
        match v {
            ErrorKind::Io => "I/O error",
            ErrorKind::External => "External error",
            ErrorKind::Config => "Config error",
            ErrorKind::ChannelClosed => "Channel closed",
            ErrorKind::TaskCancelled => "Task cancelled",
            ErrorKind::Join => "Join error",
            ErrorKind::Parse => "Parse error",
            ErrorKind::BufferSizeLimit => "Buffer size limit exceeded",
            ErrorKind::ChecksumMismatch => "Checksum mismatch",
            ErrorKind::MagicMismatch => "Magic mismatch",
            ErrorKind::OutOfRange => "Out of range",
            ErrorKind::NoSpace => "No space",
            ErrorKind::Closed => "Closed",
            ErrorKind::Recover => "Recover error",
            ErrorKind::Unsupported => "Unsupported operation",
        }
    }
}

/// Error is the error struct returned by all foyer functions.
///
/// ## Display
///
/// Error can be displayed in two ways:
///
/// - Via `Display`: like `err.to_string()` or `format!("{err}")`
///
/// Error will be printed in a single line:
///
/// ```shell
/// External error, context: { k1: v2, k2: v2 } => external error, source: TestError: test error
/// ```
///
/// - Via `Debug`: like `format!("{err:?}")`
///
/// Error will be printed in multi lines with more details and backtraces (if captured):
///
/// ```shell
/// External error => external error
///
/// Context:
///   k1: v2
///   k2: v2
///
/// Source:
///   TestError: test error
///
/// Backtrace:
///    0: foyer_common::error::Error::new
///              at ./src/error.rs:259:38
///    1: foyer_common::error::tests::test_error_format
///              at ./src/error.rs:481:17
///    2: foyer_common::error::tests::test_error_format::{{closure}}
///              at ./src/error.rs:480:27
///    ...
/// ```
///
/// - For conventional struct-style Debug representation, like `format!("{err:#?}")`:
///
/// ```shell
/// Error {
///     kind: External,
///     message: "external error",
///     context: [
///         (
///             "k1",
///             "v2",
///         ),
///         (
///             "k2",
///             "v2",
///         ),
///     ],
///     source: Some(
///         TestError(
///             "test error",
///         ),
///     ),
///     backtrace: Some(
///         Backtrace [
///             { fn: "foyer_common::error::Error::new", file: "./src/error.rs", line: 259 },
///             { fn: "foyer_common::error::tests::test_error_format", file: "./src/error.rs", line: 481 },
///             ...
///         ],
///     ),
/// }
/// ```
pub struct Error {
    kind: ErrorKind,
    message: String,

    context: Vec<(&'static str, String)>,

    source: Option<Arc<anyhow::Error>>,
    backtrace: Option<Arc<Backtrace>>,
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // If alternate has been specified, we will print like Debug.
        if f.alternate() {
            let mut de = f.debug_struct("Error");
            de.field("kind", &self.kind);
            de.field("message", &self.message);
            de.field("context", &self.context);
            de.field("source", &self.source);
            de.field("backtrace", &self.backtrace);
            return de.finish();
        }

        write!(f, "{}", self.kind)?;
        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }
        writeln!(f)?;

        if !self.context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (k, v) in self.context.iter() {
                writeln!(f, "  {}: {}", k, v)?;
            }
        }

        if let Some(source) = &self.source {
            writeln!(f)?;
            writeln!(f, "Source:")?;
            writeln!(f, "  {source:#}")?;
        }

        if let Some(backtrace) = &self.backtrace {
            writeln!(f)?;
            writeln!(f, "Backtrace:")?;
            writeln!(f, "{backtrace}")?;
        }

        Ok(())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            let mut iter = self.context.iter().peekable();
            while let Some((k, v)) = iter.next() {
                write!(f, "{}: {}", k, v)?;
                if iter.peek().is_some() {
                    write!(f, ", ")?;
                }
            }
            write!(f, " }}")?;
        }

        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref().as_ref())
    }
}

/// Cloning an [`Error`] with large message and context can be expensive.
///
/// Be careful when cloning errors in performance-critical paths.
impl Clone for Error {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            message: self.message.clone(),
            context: self.context.clone(),
            source: self.source.clone(),
            backtrace: self.backtrace.clone(),
        }
    }
}

impl Error {
    /// Create a new error.
    ///
    /// If the error needs to carry a source error, please use `with_source` method.
    ///
    /// For example"
    ///
    /// ```rust
    /// # use foyer_common::error::{Error, ErrorKind};
    /// let io_error = std::io::Error::other("an I/O error occurred");
    /// Error::new(ErrorKind::Io, "an external error occurred").with_source(io_error);
    /// ```
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            context: Vec::new(),
            source: None,
            backtrace: Some(Arc::new(Backtrace::capture())),
        }
    }

    /// Add more context in error.
    pub fn with_context(mut self, key: &'static str, value: impl ToString) -> Self {
        self.context.push((key, value.to_string()));
        self
    }

    /// Set source for error.
    ///
    /// # Notes
    ///
    /// If the source has been set, we will raise a panic here.
    pub fn with_source(mut self, source: impl Into<anyhow::Error>) -> Self {
        debug_assert!(self.source.is_none(), "the source error has been set");
        self.source = Some(Arc::new(source.into()));
        self
    }

    /// Get the error kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Get the error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Get the error context.
    pub fn context(&self) -> &Vec<(&'static str, String)> {
        &self.context
    }

    /// Get the error backtrace.
    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.backtrace.as_deref()
    }

    /// Get the error source.
    pub fn source(&self) -> Option<&anyhow::Error> {
        self.source.as_deref()
    }

    /// Downcast the reference of the source error to a specific error type reference.
    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.source.as_deref().and_then(|e| e.downcast_ref::<E>())
    }
}

/// Result type for foyer.
pub type Result<T> = std::result::Result<T, Error>;

/// Helper methods for Error.
impl Error {
    /// Helper for creating an [`ErrorKind::Io`] error from a raw OS error code.
    pub fn raw_os_io_error(raw: i32) -> Self {
        let source = std::io::Error::from_raw_os_error(raw);
        Self::io_error(source)
    }

    /// Helper for creating an [`ErrorKind::Io`] error from [`std::io::Error`].
    pub fn io_error(source: std::io::Error) -> Self {
        match source.kind() {
            std::io::ErrorKind::WriteZero => Error::new(ErrorKind::BufferSizeLimit, "coding error").with_source(source),
            _ => Error::new(ErrorKind::Io, "coding error").with_source(source),
        }
    }

    /// Helper for creating an error from [`bincode::Error`].
    #[cfg(feature = "serde")]
    pub fn bincode_error(source: bincode::Error) -> Self {
        match *source {
            bincode::ErrorKind::SizeLimit => Error::new(ErrorKind::BufferSizeLimit, "coding error").with_source(source),
            bincode::ErrorKind::Io(e) => Self::io_error(e),
            _ => Error::new(ErrorKind::External, "coding error").with_source(source),
        }
    }

    /// Helper for creating a [`ErrorKind::NoSpace`] error with context.
    pub fn no_space(capacity: usize, allocated: usize, required: usize) -> Self {
        Error::new(ErrorKind::NoSpace, "not enough space left")
            .with_context("capacity", capacity)
            .with_context("allocated", allocated)
            .with_context("required", required)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::io_error(e)
    }
}

#[cfg(feature = "serde")]
impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Self::bincode_error(e)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<Error>();
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestError(String);

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError: {}", self.0)
        }
    }

    impl std::error::Error for TestError {}

    #[test]
    fn test_error_display() {
        let io_error = std::io::Error::other("some I/O error");
        let err = Error::new(ErrorKind::Io, "an I/O error occurred")
            .with_source(io_error)
            .with_context("k1", "v1")
            .with_context("k2", "v2");

        assert_eq!(
            "I/O error, context: { k1: v1, k2: v2 } => an I/O error occurred, source: some I/O error",
            err.to_string()
        );
    }

    #[test]
    fn test_error_downcast() {
        let inner = TestError("Error or not error, that is a question.".to_string());
        let err = Error::new(ErrorKind::External, "").with_source(inner.clone());

        let downcasted = err.downcast_ref::<TestError>().unwrap();
        assert_eq!(downcasted, &inner);
    }

    #[test]
    fn test_error_format() {
        let e = Error::new(ErrorKind::External, "external error")
            .with_context("k1", "v2")
            .with_context("k2", "v2")
            .with_source(TestError("test error".into()));

        println!("========== BEGIN DISPLAY FORMAT ==========");
        println!("{e}");
        println!("========== END DISPLAY FORMAT ==========");

        println!();

        println!("========== BEGIN DEBUG FORMAT ==========");
        println!("{e:?}");
        println!("========== END DEBUG FORMAT ==========");

        println!();

        println!("========== BEGIN DEBUG FORMAT (PRETTY) ==========");
        println!("{e:#?}");
        println!("========== END DEBUG FORMAT (PRETTY) ==========");
    }
}
