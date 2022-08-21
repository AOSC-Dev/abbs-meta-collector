use crate::Config;
use anyhow::{Context, Result};
use git2::{Blob, Commit, Error, Oid, Repository as Git2Repository, TreeWalkResult};
use std::path::{Path, PathBuf};
pub mod commit;

pub struct Repository {
    repo_path: PathBuf,
    repo: git2::Repository,
    pub branch: String,
    pub tree: String,
}

pub struct SyncRepository {
    pub repo_path: PathBuf,
    pub branch: String,
    pub tree: String,
}

impl From<&Repository> for SyncRepository {
    fn from(repo: &Repository) -> Self {
        Self {
            repo_path: repo.repo_path.clone(),
            branch: repo.branch.clone(),
            tree: repo.tree.clone(),
        }
    }
}

impl TryFrom<&SyncRepository> for Repository {
    type Error = git2::Error;

    fn try_from(repo: &SyncRepository) -> Result<Self, Self::Error> {
        Self::open(&repo.repo_path, &repo.tree, &repo.branch)
    }
}

impl TryFrom<&Config> for Repository {
    type Error = anyhow::Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        let repo = Repository::open(&config.abbs_path, &config.name, &config.branch)?;
        Ok(repo)
    }
}

impl Repository {
    pub fn open<P: AsRef<Path>, S: AsRef<str>>(
        path: P,
        tree: S,
        branch: S,
    ) -> std::result::Result<Repository, git2::Error> {
        let repo = Git2Repository::open(path.as_ref())?;
        repo.find_branch(branch.as_ref(), git2::BranchType::Local)?;
        Ok(Repository {
            tree: tree.as_ref().to_string(),
            repo_path: PathBuf::from(path.as_ref()),
            repo,
            branch: branch.as_ref().to_string(),
        })
    }

    pub fn get_repo_branch(&self) -> &str {
        &self.branch
    }

    pub fn get_branch_oid(&self, branch_name: &str) -> Result<Oid> {
        let branch = self
            .repo
            .find_branch(branch_name, git2::BranchType::Local)
            .or_else(|_| self.repo.find_branch(branch_name, git2::BranchType::Remote))?;
        let branch = branch
            .into_reference()
            .target()
            .with_context(|| format!("branch {} doesn't exist", branch_name));

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
    pub fn walk_commit(&self, commit: Oid) -> Result<Vec<PathBuf>> {
        let commit = self.repo.find_commit(commit)?;
        let tree = commit.tree()?;

        let mut dirs = vec![];
        tree.walk(git2::TreeWalkMode::PostOrder, |dir, file| {
            if let Some(filename) = file.name() {
                let mut res = PathBuf::new();
                res.push(Path::new(dir));
                res.push(filename);
                dirs.push(res);
            }
            TreeWalkResult::Ok
        })
        .ok();

        Ok(dirs)
    }

    #[inline(always)]
    pub fn read_file(&self, path: impl AsRef<Path>, commit: Oid) -> Result<String> {
        let commit = self.repo.find_commit(commit)?;
        let tree = commit.tree()?;
        Ok(String::from_utf8(
            self.repo
                .find_blob(tree.get_path(path.as_ref())?.id())?
                .content()
                .to_vec(),
        )?)
    }
}
