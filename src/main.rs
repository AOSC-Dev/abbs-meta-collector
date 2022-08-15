use abbs_meta::{
    db::{abbs::AbbsDb, commits::CommitDb},
    git::Repository,
    package, Config,
};
use anyhow::{Context, Result};
use std::collections::HashMap;
use tracing::{debug, info};

#[async_std::main]
async fn main() -> Result<()> {
    init_log();

    let config = Config::from_file("config.toml")?;
    let repo = Repository::try_from(&config)?;

    let commit_db = CommitDb::open(&config.commits_db_path).await?;
    let abbs_db = AbbsDb::open(&config).await?;

    let (pkgs, errors) =
        package::scan_packages(&repo).with_context(|| "failed to scan packages")?;
    abbs_db.delete_package_errors().await?;
    abbs_db.add_package_errors(errors).await?;

    // find packages that were deleted in current abbs
    let old_pkgs = abbs_db.get_packages_name().await?;
    let current_pkgs = pkgs.iter().map(|(pkg, _)| pkg.name.clone()).collect();
    let deleted_packages = old_pkgs.difference(&current_pkgs);

    info!("deleted packages: {:?}", deleted_packages);
    abbs_db.delete_package_many(deleted_packages).await?;

    let updated_pkgs = commit_db.update(&repo).await?;
    info!("updated packages: {:?}", updated_pkgs);

    let mut map: HashMap<_, Vec<_>> = HashMap::new();
    for pkg in pkgs {
        if let Some(v) = map.get_mut(&pkg.0.name) {
            v.push(pkg);
        } else {
            map.insert(pkg.0.name.clone(), vec![pkg]);
        }
    }

    for pkg_name in updated_pkgs {
        if let Some(v) = map.get(&pkg_name) {
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
