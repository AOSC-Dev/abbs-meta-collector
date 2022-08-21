use abbs_meta::{
    db::{abbs::AbbsDb, commits::CommitDb},
    git::Repository,
    package, Config,
};
use anyhow::{Context, Result};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info};

#[async_std::main]
async fn main() -> Result<()> {
    init_log();

    let config = Config::from_file("config.toml")?;
    let repo = Repository::try_from(&config)?;
    let commit_db = CommitDb::open(&config.commits_db_path).await?;
    let abbs_db = AbbsDb::open(&config).await?;

    abbs_db
        .update_package_testing(&commit_db, &repo, &HashSet::new())
        .await?;

    let (pkgs, errors) =
        package::scan_packages(&repo, repo.get_branch_oid(repo.get_repo_branch())?)
            .with_context(|| "failed to scan packages")?;

    // update package_errors table
    abbs_db.delete_package_errors().await?;
    abbs_db.add_package_errors(errors).await?;

    // find packages that were deleted in current abbs
    let old_pkgs = abbs_db.get_packages_name().await?;
    let current_pkgs = pkgs.iter().map(|(pkg, _)| pkg.name.clone()).collect();
    let deleted_packages = old_pkgs.difference(&current_pkgs);

    info!("deleted packages: {}", deleted_packages.clone().join(" "));
    abbs_db.delete_packages(deleted_packages).await?;

    let updated_pkgs: HashSet<_> = commit_db
        .update_repo_branch(&repo)
        .await?
        .into_iter()
        .map(|c| c.pkg_name)
        .collect();

    info!("updated packages: {}", updated_pkgs.iter().join(" "));

    let mut map: HashMap<_, Vec<_>> = HashMap::new();
    for pkg in pkgs {
        if let Some(v) = map.get_mut(&pkg.0.name) {
            v.push(pkg);
        } else {
            map.insert(pkg.0.name.clone(), vec![pkg]);
        }
    }

    let len = updated_pkgs.len();
    for (cnt, pkg_name) in updated_pkgs.iter().enumerate() {
        info!("{}/{len} {pkg_name}", cnt = cnt + 1);
        if let Some(v) = map.remove(pkg_name) {
            for (pkg, context) in v {
                let changes = package::scan_package_changes(&pkg.name, &repo, &commit_db).await?;
                abbs_db.add_package(pkg, context, changes).await?;
            }
        } else {
            debug!("desperated package: {pkg_name}");
        }
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
