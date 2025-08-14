use super::entities::prelude::*;
use super::entities::{commits, histories};
use super::{CreateTable, replace_many};
use crate::db::get_full_version;
use crate::git::commit::FileStatus;
use crate::git::{Repository, SyncRepository};
use crate::package::{
    Meta, defines_path_to_spec_path, path_to_defines_path, scan_package, scan_packages,
};
use crate::skip_error;
use FileStatus::*;
use anyhow::{Result, bail};
use chrono::{DateTime, FixedOffset, Local, TimeZone};
use git2::Oid;
use indicatif::ParallelProgressIterator;
use itertools::Itertools;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use sea_orm::ActiveValue::NotSet;
use sea_orm::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ActiveModelTrait, Database, IntoActiveModel, Iterable, QueryOrder, TransactionTrait,
};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use thread_local::ThreadLocal;
use tracing::{debug, info, warn};

/// Collect git commits in database
#[derive(Debug)]
pub struct CommitDb {
    conn: DatabaseConnection,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Change {
    pub pkg_name: String,
    pub version: String,
    pub tree: String,
    pub branch: String,
    pub urgency: String,
    pub message: String,
    pub githash: String,
    pub maintainer_name: String,
    pub maintainer_email: String,
    pub timestamp: DateTimeWithTimeZone,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CommitInfo {
    pub commit_id: Oid,
    pub commit_time: DateTimeWithTimeZone,
    pub pkg_name: String,
    pub pkg_version: String,
    pub pkg_full_version: String,
    pub defines_path: String,
    pub spec_path: String,
    pub status: FileStatus,
}

/// Convert git2::Time to DataTimeWithTimeZone
fn to_datetime(time: &git2::Time) -> DateTimeWithTimeZone {
    DateTime::from_timestamp(time.seconds(), 0)
        .unwrap()
        .with_timezone(&TimeZone::from_offset(
            &FixedOffset::east_opt(time.offset_minutes() * 60).unwrap(),
        ))
}

impl CommitDb {
    pub async fn open<P: AsRef<str>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let conn = Database::connect(path).await?;

        Commits.create_table(&conn).await?;
        Histories.create_table(&conn).await?;

        info!("commit db opened");

        Ok(Self { conn })
    }

    /// Add commits from branch to database
    pub async fn add_commits(
        &self,
        repo: &Repository,
        branch: &str,
        commits: Vec<Oid>,
    ) -> Result<Vec<CommitInfo>> {
        let db = self.conn.begin().await?;
        let tree = &repo.tree;

        let sync_repo: &SyncRepository = &repo.into();
        let local_repo: ThreadLocal<Repository> = ThreadLocal::new();
        let result = repo.scan_commits(commits)?;

        info!("collecting commit info");
        // iterate each added/modified/deleted file in each commit
        let mut commit_info: Vec<_> = (&result)
            .into_par_iter()
            .progress()
            .filter_map(|(commit_id, time, file_path, file_status)| {
                let repo = local_repo.get_or(|| sync_repo.try_into().unwrap());
                let commit_id = *commit_id;
                let commit = match file_status {
                    Added | Modified => commit_id,
                    Deleted => {
                        // find parent commit where the file still exists
                        let commit = repo.find_commit(commit_id).ok()?;
                        let parents: Vec<_> = commit.parents().collect();
                        match parents.len() {
                            1 | 2 => parents[0].id(),
                            n => {
                                warn!("{n} parents in commit {commit:?}");
                                return None;
                            }
                        }
                    }
                    _ => return None,
                };

                let generate_package_commit_info = |defines_path: &PathBuf| {
                    // for each change package, create an entry in commits table
                    // read package info from the specified commit
                    let spec_path = defines_path_to_spec_path(defines_path).ok()?;
                    let (res, _) = scan_package(repo, commit_id, &spec_path, defines_path);
                    let (pkg, _) = res?;

                    let full_version = get_full_version(&pkg);

                    Some(CommitInfo {
                        commit_id,
                        commit_time: to_datetime(time),
                        pkg_name: pkg.name.clone(),
                        pkg_version: pkg.version,
                        pkg_full_version: full_version,
                        defines_path: defines_path.to_str()?.to_string(),
                        spec_path: spec_path.to_str()?.to_string(),
                        status: *file_status,
                    })
                };

                // locate defines files related to the changed file
                path_to_defines_path(repo, commit, file_path)
                    .ok()
                    .map(|path| {
                        path.iter()
                            .filter_map(generate_package_commit_info)
                            .collect_vec()
                    })
            })
            .flatten()
            .collect();

        // dedup before inserting into database
        // primary key: (pkg_name, pkg_version, tree, branch, commit_id)
        // tree and branch are common
        commit_info.sort_by(|left, right| {
            (&left.pkg_name, &left.pkg_version, &left.commit_id).cmp(&(
                &right.pkg_name,
                &right.pkg_version,
                &right.commit_id,
            ))
        });
        commit_info.dedup_by(|left, right| {
            (&left.pkg_name, &left.pkg_version, &left.commit_id)
                == (&right.pkg_name, &right.pkg_version, &right.commit_id)
        });

        info!("saving commit info to database");
        // insert to database in chunks
        let iters = commit_info
            .clone()
            .into_iter()
            .map(
                |CommitInfo {
                     commit_id,
                     commit_time,
                     pkg_name,
                     pkg_version,
                     pkg_full_version: _,
                     defines_path,
                     spec_path,
                     status,
                 }| {
                    commits::Model {
                        pkg_name,
                        pkg_version,
                        spec_path,
                        defines_path,
                        tree: tree.clone(),
                        branch: branch.to_string(),
                        commit_id: commit_id.to_string(),
                        commit_time,
                        status: status.to_string(),
                    }
                    .into_active_model()
                },
            )
            .chunks(2048);
        for iter in iters.into_iter() {
            replace_many(
                iter,
                [
                    commits::Column::PkgName,
                    commits::Column::PkgVersion,
                    commits::Column::Tree,
                    commits::Column::Branch,
                    commits::Column::CommitId,
                ],
                commits::Column::iter(),
            )
            .exec(&db)
            .await?;
        }

        db.commit().await?;
        Ok(commit_info)
    }

    // update packages from testing branches (topic branches)
    pub async fn update_package_testing(
        &self,
        repo: &Repository,
        exculde: &HashSet<String>,
    ) -> Result<HashMap<String, Vec<CommitInfo>>> {
        let branches = repo
            .get_git2repo()
            .branches(None)?
            .filter_map(|x| Some(x.ok()?.0.name().ok()??.to_string()))
            .collect_vec();

        let stable_commits = repo
            .get_commits_by_range(None, repo.get_branch_oid("stable")?)?
            .into_iter()
            .collect();

        let testing_branches = branches
            .into_iter()
            .filter_map(|name| {
                (!(name.starts_with("retro")
                    | name.starts_with("origin/retro")
                    | ["stable", "origin/HEAD", "origin/stable"].contains(&name.as_str())
                    | exculde.contains(&name)))
                .then_some(name)
            })
            .collect_vec();

        let mut result = HashMap::new();
        for testing in testing_branches.iter() {
            info!("processing testing branch {}", testing);
            // collect new commits
            let to = skip_error!(repo.get_branch_oid(testing));
            let from = self
                .get_latest_history(&repo.tree, testing)
                .await?
                .and_then(|m| Oid::from_str(&m.commit_id).ok());

            let testing_commits: HashSet<_> =
                repo.get_commits_by_range(from, to)?.into_iter().collect();

            // skip commits in stable
            let ahead = &testing_commits - &stable_commits;
            let info = self
                .add_commits(repo, testing, ahead.into_iter().collect())
                .await?;

            self.insert_history(&repo.tree, testing, to).await?;

            if !info.is_empty() {
                result.insert(testing.to_string(), info);
            }
        }

        Ok(result)
    }

    /// Get branch histories from db
    async fn get_branch_histories(
        &self,
        tree: &str,
        branch: &str,
    ) -> Result<Vec<histories::Model>> {
        Ok(Histories::find()
            .filter(histories::Column::Tree.eq(tree.to_string()))
            .filter(histories::Column::Branch.eq(branch.to_string()))
            .order_by_desc(histories::Column::Timestamp)
            .all(&self.conn)
            .await?)
    }

    /// Get latest commit history of the branch
    async fn get_latest_history(
        &self,
        tree: &str,
        branch: &str,
    ) -> Result<Option<histories::Model>> {
        Ok(Histories::find()
            .filter(histories::Column::Tree.eq(tree.to_string()))
            .filter(histories::Column::Branch.eq(branch.to_string()))
            .order_by_desc(histories::Column::Timestamp)
            .one(&self.conn)
            .await?)
    }

    /// Save history to database
    async fn insert_history(&self, tree: &str, branch: &str, commit: Oid) -> Result<()> {
        histories::ActiveModel {
            tree: Set(tree.to_string()),
            branch: Set(branch.to_string()),
            commit_id: Set(commit.to_string()),
            timestamp: Set(Local::now().fixed_offset()),
            id: NotSet,
        }
        .save(&self.conn)
        .await?;

        Ok(())
    }

    /// Update commits in stable branch
    pub async fn update_branch(&self, repo: &Repository, branch: &str) -> Result<Vec<CommitInfo>> {
        info!("save commits from branch {} to db", branch);
        // find new commits in stable branch
        // SELECT commit_id, history FROM history WHERE timestamp = (SELECT MAX(timestamp) FROM history)
        let from = self
            .get_latest_history(&repo.tree, branch)
            .await?
            .and_then(|x| Oid::from_str(&x.commit_id).ok());

        let to = repo.get_branch_oid(&repo.branch)?;
        let commits = repo.get_commits_by_range(from, to)?;
        let result = self.add_commits(repo, &repo.branch, commits).await?;

        self.insert_history(&repo.tree, &repo.branch, to).await?;

        Ok(result)
    }

    /// Find deleted/updated packages
    pub async fn get_updated_packages(
        &self,
        repo: &Repository,
        branch: &str,
    ) -> Result<(Vec<Meta>, Vec<Meta>)> {
        let histories = self.get_branch_histories(&repo.tree, branch).await?;
        // from old to new
        // we only insert one history, so the second latest one is the previous one
        let (from, to) = match histories.len() {
            0 => {
                bail!("please update branch {branch}")
            }
            1 => (None, Oid::from_str(&histories[0].commit_id)?),
            _ => (
                Some(Oid::from_str(&histories[1].commit_id)?),
                Oid::from_str(&histories[0].commit_id)?,
            ),
        };

        // compare two commits, find deleted/updated packages
        let diff: HashSet<_> = walk_diff_tree(repo, from, Some(to))?
            .into_iter()
            .filter_map(|(path, status)| {
                let path = PathBuf::from_str(&path).ok()?;
                let commit = if status == FileStatus::Deleted {
                    from?
                } else {
                    to
                };

                path_to_defines_path(repo, commit, &path)
                    .ok()
                    .map(|defines| {
                        defines.into_iter().filter_map(move |defines| {
                            let spec = defines_path_to_spec_path(&defines).ok()?;
                            Some((spec, defines, status))
                        })
                    })
            })
            .flatten()
            .collect();
        debug!("from: {from:?}  to: {to:?}");

        let deleted = diff
            .iter()
            .filter(|(_, _, status)| status == &FileStatus::Deleted)
            .map(|(spec, defines, _)| (spec, defines))
            .collect_vec();
        let updated = diff
            .iter()
            .filter(|(_, _, status)| [FileStatus::Modified, FileStatus::Added].contains(status))
            .map(|(spec, defines, _)| (spec, defines))
            .collect_vec();

        let deleted_packages = if let Some(from) = from {
            scan_packages(repo, from, deleted)
        } else {
            vec![]
        };
        let updated_packages = scan_packages(repo, to, updated);

        Ok((deleted_packages, updated_packages))
    }

    /// Collect package commit history
    pub async fn get_package_changes(
        &self,
        repo: &Repository,
        pkg_name: &str,
    ) -> Result<Vec<Change>> {
        let changes = self.get_commits_by_packages(pkg_name).await?;

        let changes = changes
            .into_iter()
            .filter_map(
                |commits::Model {
                     pkg_name,
                     pkg_version,
                     tree,
                     branch,
                     commit_id,
                     ..
                 }| {
                    let commit = repo.find_commit(Oid::from_str(&commit_id).ok()?).ok()?;
                    let message = commit.message()?.to_string();
                    let maintainer = commit.committer();
                    let branch = branch.strip_prefix("origin/").unwrap_or(branch.as_str());

                    let change = Change {
                        pkg_name,
                        version: pkg_version,
                        tree,
                        branch: branch.into(),
                        urgency: message
                            .find("security")
                            .map_or("medium", |_| "high")
                            .to_string(),
                        message: commit.message()?.to_string(),
                        githash: commit_id,
                        maintainer_name: maintainer.name()?.to_string(),
                        maintainer_email: maintainer.email()?.to_string(),
                        timestamp: to_datetime(&commit.time()),
                    };
                    Some(change)
                },
            )
            .collect();

        Ok(changes)
    }

    /// Commits are sorted by timestamp in descending order, return Vec<(commit_id,pkg_version,spec_path,defines_path)>
    pub async fn get_commits_by_packages(&self, pkg_name: &str) -> Result<Vec<commits::Model>> {
        let v = Commits::find()
            .order_by_desc(commits::Column::CommitTime)
            .filter(commits::Column::PkgName.eq(pkg_name.to_string()))
            .all(&self.conn)
            .await?;
        Ok(v)
    }
}

/// Walk and collect files changed in the diff between two commits
fn walk_diff_tree(
    repo: &Repository,
    from: Option<Oid>,
    to: Option<Oid>,
) -> Result<Vec<(String, FileStatus)>> {
    let to_tree = |oid: Option<Oid>| {
        oid.and_then(|oid| repo.find_commit(oid).ok())
            .and_then(|commit| commit.tree().ok())
    };

    let deltas = repo.get_git2repo().diff_tree_to_tree(
        to_tree(from).as_ref(),
        to_tree(to).as_ref(),
        None,
    )?;

    let res = deltas
        .deltas()
        .filter_map(|d| {
            Some((
                d.new_file().path()?.to_str()?.to_string(),
                FileStatus::from(d.status()),
            ))
        })
        .collect_vec();
    Ok(res)
}
