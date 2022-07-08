use super::entities::{self, commit, history, Commit, History};
use crate::git::commit::FileStatus;
use crate::git::Repository;
use anyhow::{Ok, Result};
use entities::history::Column::*;
use git2::Oid;
use sea_orm::sea_query::Query;
use sea_orm::ActiveValue::NotSet;
use sea_orm::{ActiveModelTrait, Database, QueryOrder, TransactionTrait};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter, Schema, Set,
};

use std::collections::HashSet;

use commit::Column::*;
use itertools::Itertools;
use std::time::{SystemTime, UNIX_EPOCH};
use FileStatus::*;

pub struct CommitDb {
    conn: DatabaseConnection,
}

async fn create_table(conn: &DatabaseConnection, entity: impl EntityTrait) -> Result<()> {
    let builder = conn.get_database_backend();
    let schema = Schema::new(builder);

    let mut commits_table = schema.create_table_from_entity(entity);
    commits_table.if_not_exists();

    let state = builder.build(&commits_table);

    conn.execute(state).await?;

    Ok(())
}

impl CommitDb {
    pub async fn open(path: &str) -> Result<Self> {
        let conn = Database::connect("sqlite://".to_string() + path + "?mode=rwc").await?;

        create_table(&conn, commit::Entity).await?;
        create_table(&conn, history::Entity).await?;

        Ok(CommitDb { conn })
    }

    // return updated packages
    pub async fn update(&self, repo: &Repository) -> Result<HashSet<String>> {
        let db = &self.conn;

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

        let result: HashSet<_> = result
            .into_iter()
            .filter_map(|(commit, time, path, status)| {
                let mut iter = path.iter();
                let root = iter.next()?.to_str()?;

                if root.contains("core-") | root.contains("extra-") | root.contains("base-") {
                    let pkg_name = iter.next()?.to_str()?.to_string();
                    let pkg_path = root.to_string() + "/" + &pkg_name;
                    Some((commit, time.seconds(), status, pkg_name, pkg_path))
                } else {
                    None
                }
            })
            .collect();

        let updated_packages: HashSet<_> = result
            .iter()
            .map(|(_, _, _, pkg_name, _)| pkg_name.clone())
            .collect();

        // fix too many SQL variables error
        let txn = db.begin().await?;
        for x in result.iter().chunks(4096).into_iter() {
            let models: Vec<_> = x
                .map(
                    |(commit_id, time, status, pkg_name, pkg_path)| commit::ActiveModel {
                        commit_id: Set(commit_id.to_string()),
                        commit_time: Set(*time),
                        status: Set(status.to_string()),
                        pkg_name: Set(pkg_name.clone()),
                        pkg_path: Set(pkg_path.clone()),
                        id: NotSet,
                    },
                )
                .collect();

            Commit::insert_many(models).exec(&txn).await?;
        }

        history::ActiveModel {
            commit_id: Set(repo.get_head_id()?.to_string()),
            timestamp: Set(unix_timestamp_now()?),
            id: NotSet,
        }
        .save(&txn)
        .await?;

        txn.commit().await?;

        Ok(updated_packages)
    }

    /// commits are sorted by timestamp in descending order
    pub async fn get_package_commits(&self, pkg_name: &str) -> Result<Vec<(Oid, String)>> {
        let v = Commit::find()
            .order_by_desc(CommitTime)
            .filter(PkgName.eq(pkg_name))
            .all(&self.conn)
            .await?;

        let mut map = indexmap::IndexMap::new();

        for commit in v {
            if [Added, Modified].contains(&FileStatus::from(commit.status.as_str())) {
                let oid = Oid::from_str(&commit.commit_id)?;
                map.insert(oid, commit.pkg_path);
            }
        }

        Ok(map.into_iter().collect_vec())
    }
}

fn unix_timestamp_now() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64)
}
