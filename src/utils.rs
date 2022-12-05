use std::{
    io,
    path::{Path, PathBuf},
};

pub(super) fn canonicalize_blocking(path: &Path) -> io::Result<PathBuf> {
    path.canonicalize().map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("{} while canonicalizing {}", err, path.display()),
        )
    })
}
