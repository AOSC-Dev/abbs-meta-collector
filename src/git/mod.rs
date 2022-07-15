use anyhow::{Ok, Result};
use git2::{Blob, Commit, Error, Oid, Repository as Git2Repository};
use std::path::{Path, PathBuf};

use crate::Config;

pub mod commit;

pub struct Repository {
    repo_path: PathBuf,
    thread: usize,
    repo: git2::Repository,
    branch: String,
}

impl TryFrom<&Config> for Repository {
    type Error = anyhow::Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        let repo = Repository::open(&config.abbs_path, config.thread, &config.branch)?;
        Ok(repo)
    }
}

impl Repository {
    pub fn open<P: AsRef<Path>, S: AsRef<str>>(
        path: P,
        thread: usize,
        branch: S,
    ) -> Result<Repository> {
        let repo = Git2Repository::open(path.as_ref())?;
        repo.find_branch(branch.as_ref(), git2::BranchType::Local)?;
        Ok(Repository {
            repo_path: PathBuf::from(path.as_ref()),
            thread,
            repo,
            branch: branch.as_ref().to_string(),
        })
    }

    pub fn get_branch(&self) -> &str {
        &self.branch
    }

    pub fn find_commit(&self, oid: Oid) -> Result<Commit<'_>, Error> {
        self.repo.find_commit(oid)
    }

    pub fn find_blob(&self, oid: Oid) -> Result<Blob<'_>, Error> {
        self.repo.find_blob(oid)
    }

    pub fn get_git2repo(&self) -> &Git2Repository {
        &self.repo
    }
}
