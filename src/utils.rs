use std::{
    io,
    path::{Path, PathBuf},
};

fn format_err(err: io::Error, path: &Path) -> io::Error {
    io::Error::new(
        err.kind(),
        format!("{} while canonicalizing {}", err, path.display()),
    )
}

pub(super) fn canonicalize_blocking(path: &Path) -> io::Result<PathBuf> {
    path.canonicalize().map_err(|err| format_err(err, path))
}

pub(super) async fn canonicalize(path: &Path) -> io::Result<PathBuf> {
    tokio::fs::canonicalize(path)
        .await
        .map_err(|err| format_err(err, path))
}
