use std::hash::Hash;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{fs, io, path};

use anyhow::Context;
use cairo_lang_project::PROJECT_FILE_NAME;
use cairo_lang_utils::{define_short_id, Intern, LookupIntern};
use salsa::Durability;
use tracing::{error, warn};
use xxhash_rust::xxh3::xxh3_64;

use crate::toolchain::scarb::{SCARB_LOCK, SCARB_TOML};

/// An opaque wrapper over a [`Path`] that refers to a file that is relevant for project analysis.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Digestible(Arc<Path>);

impl Digestible {
    /// Creates new digestible from a given path.
    ///
    /// Returns `Some` if a path points to a file that is relevant for project analysis; otherwise,
    /// returns `None`.
    pub fn try_new(path: &Path) -> Option<Self> {
        if let PROJECT_FILE_NAME | SCARB_TOML | SCARB_LOCK = path.file_name()?.to_str()? {
            let abs = path::absolute(path)
                .context("failed to find absolute path")
                .with_context(|| format!("failed to find absolute path to: {}", path.display()))
                .unwrap_or_else(|err| {
                    warn!("{err:?}");
                    path.to_owned()
                });
            Some(Self(abs.into()))
        } else {
            None
        }
    }
}

define_short_id!(DigestId, Digestible, LsDigestsGroup, lookup_intern_digest, intern_digest);

/// An opaque object carrying digests of files.
///
/// Digests carry internally one of three state kinds:
/// 1. File exists and has specific content hash.
/// 2. File does not exist.
/// 3. Some I/O error occurs when trying to read the file, each error occurrence yields a new digest
///    instance.
///
/// By being computed and passed through Salsa queries, digests are used as a mean of cache-busting
/// when the contents of relevant files change.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Digest(DigestKind);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
enum DigestKind {
    Ok(u64),
    FileNotFound,
    IoError(usize),
}

impl Digest {
    fn ok(hash: u64) -> Self {
        Self(DigestKind::Ok(hash))
    }

    fn file_not_found() -> Self {
        Self(DigestKind::FileNotFound)
    }

    fn io_error() -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        Self(DigestKind::IoError(count))
    }
}

/// A group of queries for tracking [`Digest`]s of files.
#[salsa::query_group(LsDigestsDatabase)]
pub trait LsDigestsGroup {
    #[salsa::interned]
    fn intern_digest(&self, path: Digestible) -> DigestId;

    /// Compute digest of a digestible file.
    fn digest(&self, digest: DigestId) -> Digest;
}

/// Tell Salsa that executing this query depends on reading the contents of the given file.
///
/// The file path is expected to be digestible, an error will be logged otherwise.
pub fn report_digest_dependency(db: &dyn LsDigestsGroup, path: &Path) {
    let Some(digestible) = Digestible::try_new(path) else {
        error!(
            "project model attempts to report dependency on indigestible file: {}",
            path.display()
        );
        return;
    };

    let digest = digestible.intern(db);
    db.digest(digest);
}

/// Invalidates the digest of the given digestible file, forcing the project database to recompute
/// it on the next query computation.
pub fn invalidate_digest(db: &mut dyn LsDigestsGroup, digest: DigestId) {
    DigestQuery.in_db_mut(db).invalidate(&digest);
}

fn digest(db: &dyn LsDigestsGroup, digest: DigestId) -> Digest {
    let Digestible(path) = digest.lookup_intern(db);
    db.salsa_runtime().report_synthetic_read(Durability::LOW);
    match fs::read(&*path) {
        Ok(bytes) => Digest::ok(xxh3_64(&bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Digest::file_not_found(),
        Err(_) => Digest::io_error(),
    }
}
