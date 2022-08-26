use abbs_meta::{
    config::{Config, Global, Repo},
    db::{abbs::AbbsDb, commits::CommitDb},
    git::Repository,
};
use anyhow::Result;
use itertools::Itertools;
use std::collections::HashSet;
use tracing::info;

#[async_std::main]
async fn main() -> Result<()> {
    init_log();

    let Config {
        ref global,
        repo: ref repos,
    } = Config::from_file("config.toml")?;

    for repo in repos {
        info!("scan: {}/{}", repo.name, repo.branch);
        do_scan_and_update(global, repo).await?;
    }

    Ok(())
}

pub async fn do_scan_and_update(global_config: &Global, repo_config: &Repo) -> Result<()> {
    let repo = &(Repository::open(global_config, repo_config)?);
    let commit_db = &(CommitDb::open(&global_config.commits_db_path).await?);
    let abbs_db = &(AbbsDb::open(global_config, repo_config).await?);
    abbs_db
        .update_testing_branch(commit_db, repo, &HashSet::new())
        .await?;
    commit_db.update_branch(repo, &repo.branch).await?;

    let (deleted, updated) = commit_db.get_updated_packages(repo, &repo.branch).await?;

    let deleted = deleted
        .into_iter()
        .map(|(pkg, _, _)| pkg.name)
        .collect_vec();
    let sep = if !deleted.is_empty() { ":" } else { "" };
    info!(
        "delete {} packages{} {}",
        deleted.len(),
        sep,
        deleted.join(" ")
    );
    info!("update {} packages", updated.len());
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
