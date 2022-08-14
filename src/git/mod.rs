use crate::Config;
use anyhow::{Context, Ok, Result};
use git2::{Blob, Commit, Error, Oid, Repository as Git2Repository};
use std::path::{Path, PathBuf};
pub mod commit;

pub struct Repository {
    repo_path: PathBuf,
    repo: git2::Repository,
    branch: String,
}

impl TryFrom<&Config> for Repository {
    type Error = anyhow::Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        let repo = Repository::open(&config.abbs_path, &config.branch)?;
        Ok(repo)
    }
}

impl Repository {
    pub fn open<P: AsRef<Path>, S: AsRef<str>>(path: P, branch: S) -> Result<Repository> {
        let repo = Git2Repository::open(path.as_ref())?;
        repo.find_branch(branch.as_ref(), git2::BranchType::Local)?;
        Ok(Repository {
            repo_path: PathBuf::from(path.as_ref()),
            repo,
            branch: branch.as_ref().to_string(),
        })
    }

    pub fn get_branch(&self) -> &str {
        &self.branch
    }

    pub fn get_branch_oid(&self) -> Result<Oid> {
        let branch = self
            .repo
            .find_branch(&self.branch, git2::BranchType::Local)?;
        let branch = branch
            .into_reference()
            .target()
            .with_context(|| format!("branch {} doesn't exist", self.branch));

        branch
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
