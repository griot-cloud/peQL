//! Whole-file writes a reader never sees half of, safe for several writers on one disk.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Numbers each temporary file this process writes.
static NEXT: AtomicU64 = AtomicU64::new(0);

/// Write `bytes` to `path` through a temporary file of this writer's own, renamed into
/// place. Engines on one disk (several processes over one workspace) write the same file at once; a
/// shared temporary name lets one rename the other's file away before it renames its own.
pub(crate) fn write(path: &Path, bytes: impl AsRef<[u8]>) -> std::io::Result<()> {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        // The rename's error is the one reported; the temporary file is not left behind.
        let _ = std::fs::remove_file(&tmp);
    })
}
