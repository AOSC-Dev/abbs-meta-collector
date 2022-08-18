use super::entities::prelude::*;
use super::entities::{commit, history};
use super::{replace_many, CreateTable};
use crate::git::commit::FileStatus;
use crate::git::Repository;
use crate::skip_error;
use crate::skip_none;
use abbs_meta_apml::parse;
use anyhow::Result;
use git2::{Oid, TreeWalkResult};
use indexmap::IndexSet;
use itertools::Itertools;
use rayon::prelude::IntoParallelIterator;
use sea_orm::sea_query::Query;
use sea_orm::ActiveValue::NotSet;
use sea_orm::{ActiveModelTrait, Database, QueryOrder, TransactionTrait};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use FileStatus::*;

#[derive(Debug)]
pub struct CommitDb {
    conn: DatabaseConnection,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CommitInfo {
    pub commit_id: String,
    pub time: i64,
    pub pkg_name: String,
    pub pkg_version: String,
    pub defines_path: String,
    pub spec_path: String,
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
    ) -> Result<HashSet<CommitInfo>> {
        let result = repo.scan_commits(commits)?;
        let db = self.conn.begin().await?;

        let mut commit_info = HashSet::new();
        for (commit_id, time, file_path, file_status) in result {
            if ![Added, Modified].contains(&file_status) {
                continue;
            }
            let commit = skip_error!(repo.find_commit(commit_id));
            let tree = skip_error!(commit.tree());

            let mut insert_package_commit_info = |defines_path: PathBuf| -> Option<()> {
                let defines_path_to_spec_path = |path: &Path| -> Option<PathBuf> {
                    let mut spec_path = path.parent()?.parent()?.to_path_buf();
                    spec_path.push("spec");
                    Some(spec_path)
                };

                let spec_path = defines_path_to_spec_path(&defines_path)?;
                let mut context = HashMap::new();
                let get_file_content = |path| -> Option<String> {
                    String::from_utf8(
                        repo.find_blob(tree.get_path(path).ok()?.id())
                            .ok()?
                            .content()
                            .to_vec(),
                    )
                    .ok()
                };

                let spec = get_file_content(&spec_path)?;
                let defines = get_file_content(&defines_path)?;

                // just ignore parse error
                parse(&spec, &mut context).ok();
                parse(&defines, &mut context).ok();

                let pkg_name = context.get("PKGNAME")?.clone();
                let pkg_version = context.get("VER")?.clone();
                let spec_path = spec_path.to_str()?.to_string();
                let defines_path = defines_path.to_str()?.to_string();

                commit_info.insert(CommitInfo {
                    commit_id: commit_id.to_string(),
                    time: time.seconds(),
                    pkg_name,
                    pkg_version,
                    defines_path,
                    spec_path,
                });
                Some(())
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

            let file_name = skip_none!(skip_none!(file_path.file_name()).to_str());
            match file_name {
                "defines" => {
                    skip_none!(insert_package_commit_info(file_path))
                }
                "spec" => {
                    let pkg_path = skip_none!(file_path.parent());
                    for path in skip_none!(walk(pkg_path)) {
                        if path.file_name() == Some(OsStr::new("defines")) {
                            insert_package_commit_info(path);
                        }
                    }
                }
                _ => {
                    for path in file_path.ancestors() {
                        let mut path = path.to_path_buf();
                        path.push(Path::new("defines"));
                        if tree.get_path(&path).is_ok() {
                            insert_package_commit_info(path);
                        }
                    }
                }
            };
        }

        let iters = commit_info
            .clone()
            .into_iter()
            .map(
                |CommitInfo {
                     commit_id,
                     time,
                     pkg_name,
                     pkg_version,
                     defines_path,
                     spec_path,
                 }| {
                    commit::ActiveModel {
                        tree: Set(repo.tree.clone()),
                        branch: Set(branch.as_ref().to_string()),
                        commit_id: Set(commit_id),
                        commit_time: Set(time),
                        pkg_name: Set(pkg_name),
                        pkg_version: Set(pkg_version),
                        spec_path: Set(spec_path),
                        defines_path: Set(defines_path),
                        id: NotSet,
                    }
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
    ) -> Result<HashMap<String, HashSet<CommitInfo>>> {
        use anyhow::Context;
        use git2::Branch;

        let branches = repo
            .get_git2repo()
            .branches(None)?
            .filter_map(|x| x.ok())
            .map(|x| x.0)
            .collect_vec();

        let get_head_oid = |branch: &Branch| {
            branch
                .get()
                .target()
                .with_context(|| format!("failed to find commit of branch {:?}", branch.name()))
        };

        let stable = branches
            .iter()
            .find(|b| Ok(Some("stable")) == b.name())
            .with_context(|| "there is no stable branch")?;
        let stable_commits = repo
            .get_commits_by_range(get_head_oid(stable)?, None)?
            .into_iter()
            .collect();

        let testing_branches = branches
            .into_iter()
            .filter_map(|b| {
                if let Ok(Some(name)) = b.name() {
                    if (name == "stable")
                        | (name == "origin/HEAD")
                        | (name == "origin/stable")
                        | name.starts_with("retro")
                        | name.starts_with("origin/retro")
                        | exculde.contains(name)
                    {
                        return None;
                    }
                }
                Some(b)
            })
            .collect_vec();

        let mut result = HashMap::new();

        for testing in testing_branches {
            let from = get_head_oid(&testing)?;
            let branch = testing
                .name()?
                .with_context(|| "failed to parse branch name")?;
            let to = self
                .get_latest_history(&repo.tree, branch)
                .await?
                .and_then(|m| Oid::from_str(&m.commit_id).ok());

            let testing_commits: HashSet<_> =
                repo.get_commits_by_range(from, to)?.into_iter().collect();

            let ahead = &testing_commits - &stable_commits;
            let info = self.add_commits(repo, branch, ahead).await?;

            self.insert_history(&repo.tree, branch, from).await?;

            if !info.is_empty() {
                result.insert(branch.to_string(), info);
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
    pub async fn update_repo_branch(&self, repo: &Repository) -> Result<HashSet<CommitInfo>> {
        // SELECT commit_id, history FROM history WHERE timestamp = (SELECT MAX(timestamp) FROM history)
        let to = self
            .get_latest_history(&repo.tree, &repo.branch)
            .await?
            .and_then(|x| Oid::from_str(&x.commit_id).ok());

        let from = repo.get_branch_oid()?;
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
