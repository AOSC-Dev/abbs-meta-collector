use super::create_table;
use super::entities::prelude::*;
use super::entities::{self, commit, history};
use crate::git::commit::FileStatus;
use crate::git::Repository;
use crate::skip_error;
use crate::skip_none;
use abbs_meta_apml::parse;
use anyhow::Result;
use commit::Column::*;
use entities::history::Column::*;
use git2::{Oid, TreeWalkResult};
use itertools::Itertools;
use sea_orm::sea_query::Query;
use sea_orm::ActiveValue::NotSet;
use sea_orm::{ActiveModelTrait, Database, QueryOrder, TransactionTrait};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use FileStatus::*;

pub struct CommitDb {
    conn: DatabaseConnection,
}

impl CommitDb {
    pub async fn open<P: AsRef<str>>(path: P) -> Result<Self> {
        let conn = Database::connect("sqlite://".to_string() + path.as_ref() + "?mode=rwc").await?;

        create_table(&conn, commit::Entity).await?;
        create_table(&conn, history::Entity).await?;

        Ok(Self { conn })
    }

    pub async fn update(&self, repo: &Repository) -> Result<HashSet<String>> {
        let txn = self.conn.begin().await?;
        let db = &txn;

        // SELECT commit_id, history FROM history WHERE timestamp = (SELECT MAX(timestamp) FROM history)
        let last_commit = History::find()
            .filter(
                Timestamp.in_subquery(
                    Query::select()
                        .expr(Timestamp.max())
                        .from(History)
                        .to_owned(),
                ),
            )
            .one(db)
            .await?;

        let to: Option<Oid> = last_commit.and_then(|x| Oid::from_str(&x.commit_id).ok());
        let result = repo.scan_commits(to)?;

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

                commit_info.insert((
                    commit_id.to_string(),
                    time.seconds(),
                    pkg_name,
                    pkg_version,
                    defines_path,
                    spec_path,
                ));
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

        let updated_packages = commit_info
            .iter()
            .map(|(_, _, pkg_name, _, _, _)| pkg_name.clone())
            .collect();

        let iters = commit_info
            .into_iter()
            .map(
                |(commit_id, time, pkg_name, pkg_version, defines_path, spec_path)| {
                    commit::ActiveModel {
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
            .chunks(4096);
        for iter in iters.into_iter() {
            Commit::insert_many(iter).exec(db).await?;
        }

        history::ActiveModel {
            commit_id: Set(repo.get_branch_oid()?.to_string()),
            timestamp: Set(unix_timestamp_now()?),
            id: NotSet,
        }
        .save(&txn)
        .await?;

        txn.commit().await?;

        Ok(updated_packages)
    }

    /// commits are sorted by timestamp in descending order, return Vec<(commit_id,pkg_version,spec_path,defines_path)>
    pub async fn get_package_commits(
        &self,
        pkg_name: &str,
    ) -> Result<Vec<(Oid, String, String, String)>> {
        let v = Commit::find()
            .order_by_desc(CommitTime)
            .filter(PkgName.eq(pkg_name))
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
}

fn unix_timestamp_now() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64)
}
