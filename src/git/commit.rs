use super::Repository;
use anyhow::Result;
use git2::{Delta, Oid, Time};
use rayon::prelude::*;
use std::path::PathBuf;
use thread_local::ThreadLocal;
use tracing::warn;

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
    pub fn get_commits_by_range(&self, from: Oid, to: Option<Oid>) -> Result<Vec<Oid>> {
        let mut revwalk = self.repo.revwalk()?;
        revwalk.push(from)?;

        let mut oids = vec![];

        for oid in revwalk {
            let oid = oid?;
            if Some(oid) != to {
                oids.push(oid);
            } else {
                break;
            }
        }

        Ok(oids)
    }

    pub fn scan_commits(
        &self,
        oids: impl IntoParallelIterator<Item = Oid>,
    ) -> Result<Vec<(Oid, Time, PathBuf, FileStatus)>> {
        let repo_path = &self.repo_path.clone();

        let repo: ThreadLocal<git2::Repository> = ThreadLocal::new();
        let result = oids
            .into_par_iter()
            .filter_map(|oid| {
                let repo = repo.get_or(|| git2::Repository::open(repo_path).unwrap());
                let commit = repo.find_commit(oid).ok()?;

                let parents: Vec<_> = commit.parents().collect();

                let parent_tree = match parents.len() {
                    0 => None,
                    1 | 2 => Some(parents[0].tree().ok()?),
                    n => {
                        warn!("{n} parents in commit {commit:?}");
                        return None;
                    }
                };
                let parent_tree = parent_tree.as_ref();

                let diff = repo
                    .diff_tree_to_tree(parent_tree, Some(&commit.tree().ok()?), None)
                    .ok()?;

                let mut v = vec![];
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
                Some(v)
            })
            .flatten()
            .collect();

        Ok(result)
    }
}
