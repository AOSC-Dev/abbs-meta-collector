use abbs_meta::{
    db::{abbs::AbbsDb, commits::CommitDb},
    git::Repository,
    Config,
};
use anyhow::Result;
use itertools::Itertools;
use std::collections::HashSet;
use tracing::info;

#[async_std::main]
async fn main() -> Result<()> {
    init_log();

    let config = Config::from_file("config.toml")?;
    let repo = &(Repository::try_from(&config)?);
    let commit_db = &(CommitDb::open(&config.commits_db_path).await?);
    let abbs_db = &(AbbsDb::open(&config).await?);

    abbs_db
        .update_testing_branch(commit_db, repo, &HashSet::new())
        .await?;
    commit_db.update_branch(repo, &repo.branch).await?;

    let (deleted, updated) = commit_db.get_updated_packages(repo, &repo.branch).await?;

    let deleted = deleted
        .into_iter()
        .map(|(pkg, _, _)| pkg.name)
        .collect_vec();
    info!("delete {} packages: {}",deleted.len(),deleted.join(" "));
    info!("update {} packages",updated.len());
    abbs_db.delete_packages(deleted).await?;

    let len = updated.len();
    for (i, pkg_meta) in updated.into_iter().enumerate() {
        let pkg_name = pkg_meta.0.name.clone();
        let pkg_changes = commit_db.get_package_changes(repo, &pkg_name).await?;
        abbs_db.add_package(pkg_meta, pkg_changes).await?;
        info!("{}/{} {}", i + 1, len, pkg_name);
    }

    Ok(())
}

fn init_log() {
    tracing_subscriber::fmt()
        .with_env_filter("sqlx::query=warn,abbs_meta=info")
        .with_file(true)
        .with_line_number(true)
        .init();
}
