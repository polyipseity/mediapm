//! Error taxonomy for CAS operations.
//!
//! Error variants are intentionally explicit so callers can distinguish between:
//! data-shape corruption, graph invariants, collision guards, and operational
//! failures such as filesystem I/O.

use std::path::PathBuf;

use miette::Diagnostic;
use thiserror::Error;

use crate::hash::Hash;

/// Hash parsing failures.
#[derive(Debug, Error, Clone, PartialEq, Eq, Diagnostic)]
pub enum HashParseError {
    /// Hash string does not satisfy expected grammar.
    #[error("invalid hash format: {0}")]
    InvalidFormat(String),
    /// Hash hex length mismatch.
    #[error("invalid digest hex length: expected {expected}, got {got}")]
    InvalidHexLength {
        /// Expected digest hex length.
        expected: usize,
        /// Actual digest hex length.
        got: usize,
    },
    /// Hash string contains an invalid hex character.
    #[error("invalid hex character: {0}")]
    InvalidHexCharacter(char),
    /// Unknown numeric algorithm code.
    #[error("unknown hash algorithm code: {0}")]
    UnknownAlgorithmCode(u64),
    /// Unknown textual algorithm name.
    #[error("unknown hash algorithm name: {0}")]
    UnknownAlgorithmName(String),
    /// Multihash parse/wrap failure.
    #[error("multihash error: {0}")]
    Multihash(String),
    /// Digest size did not match algorithm requirement.
    #[error("invalid digest size for {algorithm}: expected {expected}, got {got}")]
    InvalidDigestSize {
        /// Algorithm name.
        algorithm: &'static str,
        /// Expected digest byte length.
        expected: usize,
        /// Actual digest byte length.
        got: usize,
    },
}

/// Errors produced by CAS operations.
#[derive(Debug, Error, Diagnostic)]
pub enum CasError {
    /// The requested hash does not exist in the store.
    #[error("object not found: {0}")]
    NotFound(Hash),
    /// Constraint requests must contain at least one possible base hash.
    #[error("constraint set must contain at least one potential base")]
    EmptyConstraintSet,
    /// Constraint input violates graph or target rules.
    #[error("invalid constraint: {0}")]
    InvalidConstraint(String),
    /// User- or caller-provided input failed validation or conversion.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// Hash collision suspicion based on equal hash but different content lengths.
    #[error(
        "hash collision suspected for {hash}: existing_len={existing_len}, candidate_len={candidate_len}"
    )]
    HashCollisionLengthMismatch {
        /// The hash where length mismatch occurred.
        hash: Hash,
        /// Existing recorded byte length.
        existing_len: u64,
        /// Candidate/new byte length.
        candidate_len: u64,
    },
    /// Delta payload could not be decoded or applied.
    #[error("invalid delta payload: {0}")]
    InvalidDelta(String),
    /// Codec-level encode/decode failure distinct from generic internal errors.
    #[error("codec error: {0}")]
    Codec(String),
    /// Protocol-level request/response mismatch or invalid wire contract.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Runtime invariant check failed.
    #[error("invariant violation: {0}")]
    InvariantViolation(String),
    /// A cycle was detected in diff-base references.
    #[error("cycle detected while reconstructing {target}: {detail}")]
    CycleDetected {
        /// Hash currently being reconstructed.
        target: Hash,
        /// Extra cycle context.
        detail: String,
    },
    /// Persistent object bytes failed structural validation.
    #[error("corrupt object payload: {0}")]
    CorruptObject(String),
    /// Persistent index file failed structural validation.
    #[error("corrupt index state: {0}")]
    CorruptIndex(String),
    /// Filesystem I/O failed for a specific operation.
    #[error("io error while {operation} at {path}: {source}")]
    Io {
        /// Operation context.
        operation: String,
        /// Path involved.
        path: PathBuf,
        /// Underlying source error.
        #[source]
        source: std::io::Error,
    },
    /// I/O failed while reading or writing an unbound stream.
    #[error("stream io error while {operation}: {source}")]
    StreamIo {
        /// Operation context.
        operation: String,
        /// Underlying source error.
        #[source]
        source: std::io::Error,
    },
    /// Actor RPC transport failed for a specific operation.
    #[error("actor rpc error while {operation}: {detail}")]
    ActorRpc {
        /// Operation context.
        operation: String,
        /// Underlying actor transport detail.
        detail: String,
    },
    /// One-way actor message send failed for a specific operation.
    #[error("actor message send failed while {operation}: {detail}")]
    ActorMessage {
        /// Operation context.
        operation: String,
        /// Underlying send failure detail.
        detail: String,
    },
    /// Semaphore acquisition failed because the semaphore is closed.
    #[error("semaphore closed while {operation}: {detail}")]
    SemaphoreClosed {
        /// Operation context.
        operation: String,
        /// Underlying semaphore failure detail.
        detail: String,
    },
    /// Blocking task join failed for a specific operation.
    #[error("blocking task join failed while {operation}: {source}")]
    TaskJoin {
        /// Operation context.
        operation: String,
        /// Underlying join failure.
        #[source]
        source: tokio::task::JoinError,
    },
    /// A synchronization primitive was poisoned.
    #[error("poisoned lock '{lock}' while {operation}: {detail}")]
    PoisonedLock {
        /// Lock identifier.
        lock: String,
        /// Operation context.
        operation: String,
        /// Poison detail text.
        detail: String,
    },
    /// JSON serialization/deserialization failure.
    #[error("json serialization error: {0}")]
    Json(String),
    /// Redb transaction or table operation failure.
    #[error("redb persistence error: {0}")]
    Redb(String),
    /// Put was rejected because disk pressure exceeded hard threshold.
    #[error(
        "out of space threshold exceeded (available={available_bytes} bytes, cas_size={cas_size_bytes} bytes)"
    )]
    OutOfSpace {
        /// Free bytes currently available on partition.
        available_bytes: u64,
        /// Current total CAS store size in bytes.
        cas_size_bytes: u64,
    },
    /// Hash parse or algorithm mapping failure.
    #[error(transparent)]
    HashParse(#[from] HashParseError),
    /// Internal synchronization or state error.
    #[error("internal CAS error: {0}")]
    Internal(String),
}

impl CasError {
    /// Builds an [`CasError::Io`] with operation and path context.
    #[must_use]
    pub fn io(
        operation: impl Into<String>,
        path: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::Io { operation: operation.into(), path: path.into(), source }
    }

    /// Builds an [`CasError::StreamIo`] with operation context.
    #[must_use]
    pub fn stream_io(operation: impl Into<String>, source: std::io::Error) -> Self {
        Self::StreamIo { operation: operation.into(), source }
    }

    /// Converts a displayable JSON failure into [`CasError::Json`].
    #[must_use]
    pub fn json(source: impl std::fmt::Display) -> Self {
        Self::Json(source.to_string())
    }

    /// Builds an [`CasError::ActorRpc`] with operation context.
    #[must_use]
    pub fn actor_rpc(operation: impl Into<String>, source: impl std::fmt::Display) -> Self {
        Self::ActorRpc { operation: operation.into(), detail: source.to_string() }
    }

    /// Builds an [`CasError::ActorMessage`] with operation context.
    #[must_use]
    pub fn actor_message(operation: impl Into<String>, source: impl std::fmt::Display) -> Self {
        Self::ActorMessage { operation: operation.into(), detail: source.to_string() }
    }

    /// Builds a [`CasError::SemaphoreClosed`] with operation context.
    #[must_use]
    pub fn semaphore_closed(operation: impl Into<String>, source: impl std::fmt::Display) -> Self {
        Self::SemaphoreClosed { operation: operation.into(), detail: source.to_string() }
    }

    /// Builds a [`CasError::TaskJoin`] with operation context.
    #[must_use]
    pub fn task_join(operation: impl Into<String>, source: tokio::task::JoinError) -> Self {
        Self::TaskJoin { operation: operation.into(), source }
    }

    /// Builds a [`CasError::PoisonedLock`] with lock and operation context.
    #[must_use]
    pub fn poisoned_lock(
        lock: impl Into<String>,
        operation: impl Into<String>,
        source: impl std::fmt::Display,
    ) -> Self {
        Self::PoisonedLock {
            lock: lock.into(),
            operation: operation.into(),
            detail: source.to_string(),
        }
    }

    /// Converts a displayable redb failure into [`CasError::Redb`].
    #[must_use]
    pub fn redb(source: impl std::fmt::Display) -> Self {
        Self::Redb(source.to_string())
    }

    /// Converts a displayable codec failure into [`CasError::Codec`].
    #[must_use]
    pub fn codec(source: impl std::fmt::Display) -> Self {
        Self::Codec(source.to_string())
    }

    /// Converts a displayable protocol failure into [`CasError::Protocol`].
    #[must_use]
    pub fn protocol(source: impl std::fmt::Display) -> Self {
        Self::Protocol(source.to_string())
    }

    /// Converts a displayable object-corruption detail into [`CasError::CorruptObject`].
    #[must_use]
    pub fn corrupt_object(detail: impl Into<String>) -> Self {
        Self::CorruptObject(detail.into())
    }

    /// Converts a displayable index-corruption detail into [`CasError::CorruptIndex`].
    #[must_use]
    pub fn corrupt_index(detail: impl Into<String>) -> Self {
        Self::CorruptIndex(detail.into())
    }

    /// Converts a displayable constraint-validation detail into [`CasError::InvalidConstraint`].
    #[must_use]
    pub fn invalid_constraint(detail: impl Into<String>) -> Self {
        Self::InvalidConstraint(detail.into())
    }

    /// Converts a displayable consistency failure into [`CasError::InvariantViolation`].
    #[must_use]
    pub fn invariant(detail: impl Into<String>) -> Self {
        Self::InvariantViolation(detail.into())
    }

    /// Converts a displayable invariant/synchronization failure into [`CasError::Internal`].
    #[must_use]
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(detail.into())
    }

    /// Converts a displayable argument/conversion failure into [`CasError::InvalidInput`].
    #[must_use]
    pub fn invalid_input(detail: impl Into<String>) -> Self {
        Self::InvalidInput(detail.into())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::Hash;

    use super::CasError;

    #[test]
    fn hash_collision_error_message_contains_lengths() {
        let hash = Hash::from_content(b"abc");
        let error =
            CasError::HashCollisionLengthMismatch { hash, existing_len: 3, candidate_len: 4 };

        let rendered = error.to_string();

        assert!(rendered.contains("existing_len=3"));
        assert!(rendered.contains("candidate_len=4"));
    }

    #[test]
    fn not_found_error_includes_hash_text() {
        let hash = Hash::from_content(b"missing");

        let rendered = CasError::NotFound(hash).to_string();

        assert!(rendered.contains("object not found"));
        assert!(rendered.contains("blake3:"));
    }

    #[test]
    fn actor_rpc_error_includes_operation_context() {
        let rendered = CasError::actor_rpc("sending rpc", "mailbox closed").to_string();

        assert!(rendered.contains("actor rpc error while sending rpc"));
        assert!(rendered.contains("mailbox closed"));
    }

    #[test]
    fn actor_message_error_includes_operation_context() {
        let rendered = CasError::actor_message("sending broadcast", "actor terminated").to_string();

        assert!(rendered.contains("actor message send failed while sending broadcast"));
        assert!(rendered.contains("actor terminated"));
    }

    #[test]
    fn semaphore_closed_error_includes_operation_context() {
        let rendered =
            CasError::semaphore_closed("acquiring storage permit", "semaphore closed").to_string();

        assert!(rendered.contains("semaphore closed while acquiring storage permit"));
        assert!(rendered.contains("semaphore closed"));
    }

    #[test]
    fn poisoned_lock_error_includes_lock_name_and_operation() {
        let rendered =
            CasError::poisoned_lock("index", "updating state", "poisoned by panic").to_string();

        assert!(rendered.contains("poisoned lock 'index'"));
        assert!(rendered.contains("updating state"));
    }

    #[test]
    fn stream_io_error_preserves_source_message() {
        let source = io::Error::new(io::ErrorKind::UnexpectedEof, "reader closed");
        let rendered = CasError::stream_io("reading request stream", source).to_string();

        assert!(rendered.contains("stream io error while reading request stream"));
        assert!(rendered.contains("reader closed"));
    }

    #[test]
    fn invalid_input_error_includes_detail() {
        let rendered =
            CasError::invalid_input("bytes conversion failed: unsupported source").to_string();

        assert!(rendered.contains("invalid input"));
        assert!(rendered.contains("bytes conversion failed"));
    }

    #[test]
    fn invariant_error_includes_detail() {
        let rendered = CasError::invariant("hash mismatch after put").to_string();

        assert!(rendered.contains("invariant violation"));
        assert!(rendered.contains("hash mismatch after put"));
    }

    #[test]
    fn corrupt_object_error_includes_detail() {
        let rendered = CasError::corrupt_object("invalid vcdiff header").to_string();

        assert!(rendered.contains("corrupt object payload"));
        assert!(rendered.contains("invalid vcdiff header"));
    }

    #[test]
    fn corrupt_index_error_includes_detail() {
        let rendered = CasError::corrupt_index("missing base metadata row").to_string();

        assert!(rendered.contains("corrupt index state"));
        assert!(rendered.contains("missing base metadata row"));
    }

    #[test]
    fn invalid_constraint_error_includes_detail() {
        let rendered = CasError::invalid_constraint("target cannot reference itself").to_string();

        assert!(rendered.contains("invalid constraint"));
        assert!(rendered.contains("target cannot reference itself"));
    }
}
