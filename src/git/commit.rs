use super::{Repository, SyncRepository};
use anyhow::Result;
use git2::{Delta, Oid, Time};
use indicatif::ParallelProgressIterator;
use itertools::Itertools;
use rayon::prelude::*;
use std::{fmt::Display, path::PathBuf};
use thread_local::ThreadLocal;
use tracing::{info, warn};

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

impl Display for FileStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Added => "Added",
                Self::Deleted => "Deleted",
                Self::Modified => "Modified",
                Self::Unsupported => "Unsupported",
            }
        )
    }
}

impl Repository {
    // from old commit to new commit
    pub fn get_commits_by_range(&self, from: Option<Oid>, to: Oid) -> Result<Vec<Oid>> {
        let mut revwalk = self.repo.revwalk()?;
        revwalk.push(to)?;

        let oids = revwalk
            .map(|oid| {
                let oid = oid.ok()?;
                from.ne(&Some(oid)).then_some(oid)
            })
            .while_some()
            .collect_vec();

        Ok(oids)
    }

    /// Scan changed files in the specified commits
    pub fn scan_commits(&self, oids: Vec<Oid>) -> Result<Vec<(Oid, Time, PathBuf, FileStatus)>> {
        info!("scanning commit info");
        let sync_repo: &SyncRepository = &self.into();
        let repo: ThreadLocal<Repository> = ThreadLocal::new();
        let result = oids
            .into_par_iter()
            .progress()
            .filter_map(|oid| {
                let repo = repo.get_or(|| sync_repo.try_into().unwrap());
                let commit = repo.find_commit(oid).ok()?;

                let parents: Vec<_> = commit.parents().collect();

                // locate parent commit and compare
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
                    .get_git2repo()
                    .diff_tree_to_tree(parent_tree, Some(&commit.tree().ok()?), None)
                    .ok()?;

                // save info for each changed file
                let changes = diff
                    .deltas()
                    .filter_map(|delta| {
                        let new_file = delta.new_file();
                        let path = new_file.path()?;
                        Some((
                            commit.id(),
                            commit.time(),
                            path.to_path_buf(),
                            delta.status().into(),
                        ))
                    })
                    .collect_vec();
                Some(changes)
            })
            .flatten()
            .collect();

        Ok(result)
    }
}
