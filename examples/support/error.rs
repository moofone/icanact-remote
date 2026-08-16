use std::net::{AddrParseError, SocketAddr};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)] // Each example uses a subset of the shared concrete variants.
pub enum Error {
    #[error(transparent)]
    Remote(#[from] icanact_remote::GossipError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Hex(#[from] hex::FromHexError),
    #[error(transparent)]
    Address(#[from] AddrParseError),
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    #[error("invalid {kind} length: expected 32 bytes, got {actual}")]
    InvalidKeyLength { kind: &'static str, actual: usize },
    #[error("no connection handle for {addr}")]
    MissingConnection { addr: SocketAddr },
    #[error("{phase} coordination channel closed")]
    CoordinationClosed { phase: &'static str },
    #[error("{phase} timed out after {seconds}s (completed {completed}/{total})")]
    TimedOut {
        phase: &'static str,
        seconds: u64,
        completed: usize,
        total: usize,
    },
    #[error("{phase} failed: {message}")]
    BenchmarkFailed {
        phase: &'static str,
        message: String,
    },
    #[error("unknown flag: {0}")]
    UnknownFlag(String),
    #[error("unexpected extra argument after server public-key path: {0}")]
    UnexpectedArgument(String),
    #[error("too many numeric arguments: expected at most 3, got {0}")]
    TooManyNumericArguments(usize),
}
