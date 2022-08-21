use super::entities::prelude::*;
use super::entities::{commit, history};
use super::{replace_many, CreateTable};
use crate::db::abbs::PackageError;
use crate::git::commit::FileStatus;
use crate::git::{Repository, SyncRepository};
use crate::package::{defines_path_to_spec_path, scan_package, Context};
use crate::skip_error;
use abbs_meta_tree::Package;
use anyhow::Result;
use git2::{Oid, TreeWalkResult};
use indexmap::IndexSet;
use itertools::Itertools;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use sea_orm::sea_query::Query;
use sea_orm::ActiveValue::NotSet;
use sea_orm::{ActiveModelTrait, Database, IntoActiveModel, QueryOrder, TransactionTrait};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thread_local::ThreadLocal;
use tracing::log::warn;

use FileStatus::*;

#[derive(Debug)]
pub struct CommitDb {
    conn: DatabaseConnection,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CommitInfo {
    pub commit_id: String,
    pub commit_time: i64,
    pub pkg_name: String,
    pub pkg_version: String,
    pub pkg: Package,
    pub errors: Vec<PackageError>,
    pub context: Context,
    pub defines_path: String,
    pub spec_path: String,
    pub file_status: FileStatus,
}

impl CommitDb {
    pub async fn open<P: AsRef<str>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let conn = Database::connect(format!("sqlite://{path}?mode=rwc")).await?;

        Commit.create_table(&conn).await?;
        History.create_table(&conn).await?;

        Ok(Self { conn })
    }

    pub async fn add_commits(
        &self,
        repo: &Repository,
        branch: impl AsRef<str>,
        commits: impl IntoParallelIterator<Item = Oid>,
    ) -> Result<Vec<CommitInfo>> {
        let db = self.conn.begin().await?;

        let sync_repo: &SyncRepository = &repo.into();
        let local_repo: ThreadLocal<Repository> = ThreadLocal::new();

        let commit_info: Vec<_> = repo
            .scan_commits(commits)?
            .into_par_iter()
            .filter_map(|(commit_id, time, file_path, file_status)| {
                let repo = local_repo.get_or(|| sync_repo.try_into().unwrap());
                let commit = match file_status {
                    Added | Modified => repo.find_commit(commit_id).ok()?,
                    Deleted => {
                        let commit = repo.find_commit(commit_id).ok()?;
                        let parents: Vec<_> = commit.parents().collect();
                        match parents.len() {
                            1 | 2 => parents[0].clone(),
                            n => {
                                warn!("{n} parents in commit {commit:?}");
                                return None;
                            }
                        }
                    }
                    _ => return None,
                };
                let tree = commit.tree().ok()?;

                let generate_package_commit_info = |defines_path: PathBuf| {
                    let spec_path = defines_path_to_spec_path(&defines_path).ok()?;

                    let (res, errors) = scan_package(repo, commit_id, &spec_path, &defines_path);
                    let (pkg, context) = res?;

                    Some(CommitInfo {
                        commit_id: commit_id.to_string(),
                        commit_time: time.seconds(),
                        pkg_name: pkg.name.clone(),
                        pkg_version: pkg.version.clone(),
                        defines_path: defines_path.to_str()?.to_string(),
                        spec_path: spec_path.to_str()?.to_string(),
                        pkg,
                        errors,
                        context,
                        file_status,
                    })
                };

                // return absolute paths
                let walk = |path| -> Option<_> {
                    let entry = tree.get_path(path).ok()?;
                    let pkg_tree = repo.get_git2repo().find_tree(entry.id()).ok()?;
                    let mut dirs = Vec::new();

                    pkg_tree
                        .walk(git2::TreeWalkMode::PostOrder, |dir, file| {
                            if let Some(filename) = file.name() {
                                let mut res = path.to_path_buf();
                                res.push(Path::new(dir));
                                res.push(filename);
                                dirs.push(res);
                            }
                            TreeWalkResult::Ok
                        })
                        .ok();
                    Some(dirs)
                };

                let file_name = file_path.file_name()?.to_str()?;
                match file_name {
                    "defines" => generate_package_commit_info(file_path),
                    "spec" => {
                        let pkg_path = file_path.parent()?;
                        for path in walk(pkg_path)? {
                            if path.file_name() == Some(OsStr::new("defines")) {
                                return generate_package_commit_info(path);
                            }
                        }
                        None
                    }
                    _ => {
                        for path in file_path.ancestors() {
                            let mut path = path.to_path_buf();
                            path.push(Path::new("defines"));
                            if tree.get_path(&path).is_ok() {
                                return generate_package_commit_info(path);
                            }
                        }
                        None
                    }
                }
            })
            .collect();

        let iters = commit_info
            .clone()
            .into_iter()
            .filter_map(
                |CommitInfo {
                     commit_id,
                     commit_time,
                     pkg_name,
                     pkg_version,
                     defines_path,
                     spec_path,
                     pkg,
                     errors,
                     context,
                     file_status,
                 }| {
                    Some(
                        commit::Model {
                            pkg_name,
                            pkg_version,
                            spec_path,
                            defines_path,
                            tree: repo.tree.clone(),
                            branch: branch.as_ref().to_string(),
                            commit_id,
                            commit_time,
                            pkg: serde_json::to_value(pkg).ok()?,
                            errors: serde_json::to_value(errors).ok()?,
                            context: serde_json::to_value(context).ok()?,
                            file_status: file_status.to_string(),
                        }
                        .into_active_model(),
                    )
                },
            )
            .chunks(2048);
        for iter in iters.into_iter() {
            replace_many(iter).exec(&db).await?;
        }

        db.commit().await?;
        Ok(commit_info)
    }

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
            .get_commits_by_range(repo.get_branch_oid("stable")?, None)?
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
            let from = skip_error!(repo.get_branch_oid(testing));
            let to = self
                .get_latest_history(&repo.tree, testing)
                .await?
                .and_then(|m| Oid::from_str(&m.commit_id).ok());

            let testing_commits: HashSet<_> =
                repo.get_commits_by_range(from, to)?.into_iter().collect();

            let ahead = &testing_commits - &stable_commits;
            let info = self.add_commits(repo, testing, ahead).await?;

            self.insert_history(&repo.tree, testing, from).await?;

            if !info.is_empty() {
                result.insert(testing.to_string(), info);
            }
        }

        Ok(result)
    }

    async fn get_latest_history(
        &self,
        tree: impl AsRef<str>,
        branch: impl AsRef<str>,
    ) -> Result<Option<history::Model>> {
        Ok(History::find()
            .filter(history::Column::Tree.eq(tree.as_ref().to_string()))
            .filter(history::Column::Branch.eq(branch.as_ref().to_string()))
            .filter(
                history::Column::Timestamp.in_subquery(
                    Query::select()
                        .from(history::Entity)
                        .expr(history::Column::Timestamp.max())
                        .and_where(history::Column::Tree.eq(tree.as_ref().to_string()))
                        .and_where(history::Column::Branch.eq(branch.as_ref().to_string()))
                        .to_owned(),
                ),
            )
            .one(&self.conn)
            .await?)
    }

    async fn insert_history(
        &self,
        tree: impl AsRef<str>,
        branch: impl AsRef<str>,
        commit: Oid,
    ) -> Result<()> {
        history::ActiveModel {
            tree: Set(tree.as_ref().to_string()),
            branch: Set(branch.as_ref().to_string()),
            commit_id: Set(commit.to_string()),
            timestamp: Set(unix_timestamp_now()?),
            id: NotSet,
        }
        .save(&self.conn)
        .await?;

        Ok(())
    }

    /// return updated packages' name
    /// branch is decided by repo.branch
    pub async fn update_repo_branch(&self, repo: &Repository) -> Result<Vec<CommitInfo>> {
        // SELECT commit_id, history FROM history WHERE timestamp = (SELECT MAX(timestamp) FROM history)
        let to = self
            .get_latest_history(&repo.tree, &repo.branch)
            .await?
            .and_then(|x| Oid::from_str(&x.commit_id).ok());

        let from = repo.get_branch_oid(&repo.branch)?;
        let commits = repo.get_commits_by_range(from, to)?;
        let result = self.add_commits(repo, repo.branch.clone(), commits).await?;

        self.insert_history(&repo.tree, &repo.branch, from).await?;

        Ok(result)
    }

    /// commits are sorted by timestamp in descending order, return Vec<(commit_id,pkg_version,spec_path,defines_path)>
    pub async fn get_commits_by_packages(
        &self,
        tree: impl AsRef<str>,
        branch: impl AsRef<str>,
        pkg_name: impl AsRef<str>,
    ) -> Result<Vec<(Oid, String, String, String)>> {
        let v = Commit::find()
            .order_by_desc(commit::Column::CommitTime)
            .filter(commit::Column::PkgName.eq(pkg_name.as_ref().to_string()))
            .filter(commit::Column::Tree.eq(tree.as_ref().to_string()))
            .filter(commit::Column::Branch.eq(branch.as_ref().to_string()))
            .all(&self.conn)
            .await?;

        let mut map = indexmap::IndexMap::new();

        for commit in v {
            let oid = Oid::from_str(&commit.commit_id)?;
            map.insert(
                oid,
                (commit.pkg_version, commit.spec_path, commit.defines_path),
            );
        }

        Ok(map
            .into_iter()
            .map(|(k, v)| (k, v.0, v.1, v.2))
            .collect_vec())
    }

    pub async fn get_commits_by_tree_and_branch(
        &self,
        tree: impl AsRef<str>,
        branch: impl AsRef<str>,
    ) -> Result<IndexSet<Oid>> {
        Ok(Commit::find()
            .filter(commit::Column::Tree.eq(tree.as_ref().to_string()))
            .filter(commit::Column::Branch.eq(branch.as_ref().to_string()))
            .all(&self.conn)
            .await?
            .into_iter()
            .filter_map(|m| Oid::from_str(&m.commit_id).ok())
            .collect())
    }
}

fn unix_timestamp_now() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64)
}
