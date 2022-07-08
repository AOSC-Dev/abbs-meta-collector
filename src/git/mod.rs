use anyhow::Result;
use git2::Repository as Git2Repository;
use std::path::{Path, PathBuf};

pub mod commit;

pub struct Repository {
    repo_path: PathBuf,
    thread: usize,
    repo: git2::Repository,
}

impl Repository {
    pub fn open<P: AsRef<Path>>(path: P, thread: usize) -> Result<Repository> {
        Ok(Repository {
            repo_path: PathBuf::from(path.as_ref()),
            thread,
            repo: Git2Repository::open(path.as_ref())?,
        })
    }
}
