use super::Repository;
use anyhow::{Context, Result};
use git2::{Delta, Oid, Time};
use rayon::prelude::*;
use std::path::PathBuf;
use tracing::warn;

macro_rules! skip_error {
    ($res:expr) => {
        match $res {
            Ok(val) => val,
            Err(_) => continue,
        }
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileStatus {
    Added,
    Deleted,
    Modified,
    Unsupported,
}

impl From<Delta> for FileStatus {
    fn from(delta: Delta) -> Self {
        match delta {
            Delta::Added => Self::Added,
            Delta::Deleted => Self::Deleted,
            Delta::Modified => Self::Modified,
            _ => Self::Unsupported,
        }
    }
}

impl From<&str> for FileStatus {
    fn from(s: &str) -> Self {
        match s {
            "Added" => Self::Added,
            "Deleted" => Self::Deleted,
            "Modified" => Self::Modified,
            _ => Self::Unsupported,
        }
    }
}

impl ToString for FileStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Added => "Added",
            Self::Deleted => "Deleted",
            Self::Modified => "Modified",
            Self::Unsupported => "Unsupported",
        }
        .to_string()
    }
}

impl Repository {
    pub fn scan_commits(&self, to: Option<Oid>) -> Result<Vec<(Oid, Time, PathBuf, FileStatus)>> {
        let repo_path = &self.repo_path.clone();
        let repo = git2::Repository::open(repo_path)?;

        let mut revwalk = repo.revwalk()?;
        revwalk.push_head()?;

        let mut oids = vec![];

        for oid in revwalk {
            let oid = oid?;
            if Some(oid) != to {
                oids.push(oid);
            } else {
                break;
            }
        }

        if oids.is_empty() {
            return Ok(vec![]);
        }

        let result: Vec<_> = oids
            .par_chunks(ceil(oids.len(), self.thread))
            .filter_map(|oids| {
                let repo = git2::Repository::open(repo_path).ok()?;
                let mut v = vec![];
                for oid in oids {
                    let commit = skip_error!(repo.find_commit(*oid));

                    let parents: Vec<_> = commit.parents().collect();

                    let parent = match parents.len() {
                        1 | 2 => &parents[0],
                        n => {
                            warn!("{n} parents in commit {commit:?}");
                            continue;
                        }
                    };

                    let diff = repo
                        .diff_tree_to_tree(
                            Some(skip_error!(&parent.tree())),
                            Some(skip_error!(&commit.tree())),
                            None,
                        )
                        .ok()?;
                    for delta in diff.deltas() {
                        let new_file = delta.new_file();
                        let path = new_file.path()?;

                        v.push((
                            commit.id(),
                            commit.time(),
                            path.to_path_buf(),
                            delta.status().into(),
                        ));
                    }
                }
                Some(v)
            })
            .flatten()
            .collect();

        Ok(result)
    }

    pub fn get_head_id(&self) -> Result<Oid> {
        let head = self.repo.head()?;
        let oid = head
            .target()
            .with_context(|| "failed to get head".to_string())?;
        Ok(oid)
    }
}

fn ceil(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}
