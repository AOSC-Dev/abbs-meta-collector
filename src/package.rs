use crate::db::abbs::ErrorType;
use crate::db::abbs::PackageError;
use crate::db::commits::CommitDb;
use crate::git::Repository;
use abbs_meta_apml::parse;
use abbs_meta_tree::Package;
use anyhow::Context as AnyhowContext;
use anyhow::Result;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::{collections::HashMap, path::PathBuf};
use tracing::log::warn;

pub type Context = HashMap<String, String>;

pub fn scan_packages(repo: &Repository) -> Option<(Vec<(Package, Context)>, Vec<PackageError>)> {
    let pkg_dirs = get_tree_dirs(repo).unwrap();
    let mut pkgs = Vec::new();
    let mut errors = vec![];

    for (spec_path, defines_path) in pkg_dirs {
        let (context, error) = parse_spec_and_defines(&spec_path, &defines_path, repo)?;
        errors.extend(error);

        match Package::from(&context, &spec_path) {
            Ok(pkg) => pkgs.push((pkg, context)),
            Err(e) => {
                let pkg_name = defines_path.iter().nth_back(2)?.to_str()?;

                // extra-doc/jade/autobuild/defines -> extra-doc/jade
                let path = defines_path.ancestors().nth(2)?.to_str()?.to_string();
                errors.push(PackageError {
                    package: pkg_name.to_string(),
                    path,
                    message: e.to_string(),
                    err_type: ErrorType::Package,
                })
            }
        }
    }

    Some((pkgs, errors))
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Change {
    pub pkg_name: String,
    pub version: String,
    pub branch: String,
    pub urgency: String,
    pub message: String,
    pub githash: String,
    pub maintainer_name: String,
    pub maintainer_email: String,
    pub timestamp: i64,
}

pub async fn scan_package_changes(
    pkg_name: &str,
    repo: &Repository,
    commit_db: &CommitDb,
) -> Result<Vec<Change>> {
    let changes = commit_db.get_package_commits(pkg_name).await.unwrap();

    let changes = changes
        .into_iter()
        .filter_map(|(commit_id, pkg_version, _, _)| {
            let commit = repo.find_commit(commit_id).ok()?;

            let githash = commit_id.to_string();
            let message = commit.message()?.to_string();
            let maintainer = commit.committer();
            let maintainer_name = maintainer.name()?.to_string();
            let maintainer_email = maintainer.email()?.to_string();
            let timestamp = commit.time().seconds();
            let version = pkg_version;
            let urgency = message
                .find("security")
                .map_or("medium", |_| "high")
                .to_string();
            let pkg_name = pkg_name.to_string();
            let branch = repo.get_branch().to_string();

            let change = Change {
                pkg_name,
                version,
                branch,
                urgency,
                message,
                githash,
                maintainer_name,
                maintainer_email,
                timestamp,
            };
            Some(change)
        })
        .collect();

    Ok(changes)
}

fn parse_spec_and_defines(
    spec_path: &PathBuf,
    defines_path: &PathBuf,
    repo: &Repository,
) -> Option<(Context, Vec<PackageError>)> {
    let spec = repo.read_file(spec_path).ok()?;
    let defines = repo.read_file(defines_path).ok()?;
    let mut context = Context::new();
    let pkg_name = defines_path.iter().nth_back(2)?.to_str()?;
    let mut errors = vec![];

    // First parse spec
    if let Err(e) = parse(&spec, &mut context) {
        let iter = e.iter().filter_map(|e| {
            Some(PackageError {
                package: pkg_name.to_string(),
                path: spec_path.to_str()?.to_string(),
                message: e.to_string(),
                err_type: ErrorType::Parse,
            })
        });
        errors.extend(iter);
    }
    // Modify context so that defines can understand
    spec_decorator(&mut context);
    // Then parse defines
    if let Err(e) = parse(&defines, &mut context) {
        let iter = e.iter().filter_map(|e| {
            Some(PackageError {
                package: pkg_name.to_string(),
                path: defines_path.to_str()?.to_string(),
                message: e.to_string(),
                err_type: ErrorType::Parse,
            })
        });
        errors.extend(iter);
    }

    Some((context, errors))
}

fn spec_decorator(c: &mut Context) {
    if let Some(ver) = c.remove("VER") {
        c.insert("PKGVER".to_string(), ver);
    }

    if let Some(rel) = c.remove("REL") {
        c.insert("PKGREL".to_string(), rel);
    }
}

fn get_tree_dirs(repo: &Repository) -> Result<Vec<(PathBuf, PathBuf)>> {
    let walker: HashSet<_> = repo.walk_branch()?.into_iter().collect();
    let mut pkg_dirs = Vec::new();
    for file in walker.iter() {
        if file.file_name() == Some(OsStr::new("defines")) {
            let pkg_dir = file
                .parent()
                .with_context(|| {
                    format!("The directory of defines file {} is root.", file.display())
                })?
                .parent()
                .with_context(|| {
                    format!(
                        "The parent directory of defines file {} is root.",
                        file.display()
                    )
                })?;

            let spec_path = pkg_dir.join("spec");
            if !walker.contains(&spec_path) {
                warn!(
                    "spec file not found in {} for {}. Skipping",
                    spec_path.display(),
                    file.display()
                );

                continue;
            }

            pkg_dirs.push((spec_path, file.to_path_buf()));
        }
    }

    Ok(pkg_dirs)
}
