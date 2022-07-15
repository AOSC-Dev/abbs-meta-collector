use crate::db::commits::CommitDb;
use crate::git::Repository;
use abbs_meta_apml::parse;
use abbs_meta_tree::Package;
use anyhow::Context;
use anyhow::Result;
use std::{
    collections::HashMap,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
};
use tracing::log::warn;

pub fn scan_packages<P: AsRef<Path>>(abbs_path: P) -> Vec<(Package, HashMap<String, String>)> {
    let pkg_dirs = get_tree_dirs(abbs_path).unwrap();
    let mut pkgs = Vec::new();

    for (spec_path, defines_path) in pkg_dirs {
        let mut spec_context = HashMap::new();

        let s = fs::read_to_string(&spec_path).unwrap();
        parse(&s, &mut spec_context).ok();

        let (context, error) = parse_spec_and_defines(&spec_path, &defines_path);
        if let Some(s) = error {
            warn!("{}", s);
        }

        match Package::from(&context, &spec_path) {
            Ok(pkg) => pkgs.push((pkg, spec_context)),
            Err(e) => warn!("{}", e),
        }
    }

    pkgs
}

#[derive(Debug)]
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
) -> (HashMap<String, String>, Option<String>) {
    let spec = fs::read_to_string(&spec_path).unwrap();
    let defines = fs::read_to_string(&defines_path).unwrap();
    let mut context = HashMap::new();
    let mut error = String::new();

    // First parse spec
    if let Err(e) = parse(&spec, &mut context) {
        let e: Vec<String> = e.iter().map(|e| e.to_string()).collect();
        error
            .write_fmt(format_args!(
                "Failed to parse spec {}: {:?}",
                spec_path.display(),
                e
            ))
            .unwrap();
    }
    // Modify context so that defines can understand
    spec_decorator(&mut context);
    // Then parse defines
    if let Err(e) = parse(&defines, &mut context) {
        let e: Vec<String> = e.iter().map(|e| e.to_string()).collect();

        error
            .write_fmt(format_args!(
                "Failed to parse defines {}: {:?}",
                defines_path.display(),
                e
            ))
            .unwrap();
    }

    if error.is_empty() {
        (context, None)
    } else {
        (context, Some(error))
    }
}

fn spec_decorator(c: &mut HashMap<String, String>) {
    if let Some(ver) = c.remove("VER") {
        c.insert("PKGVER".to_string(), ver);
    }

    if let Some(rel) = c.remove("REL") {
        c.insert("PKGREL".to_string(), rel);
    }
}

fn get_tree_dirs<P: AsRef<Path>>(path: P) -> Result<Vec<(PathBuf, PathBuf)>> {
    let walker = walkdir::WalkDir::new(path).max_depth(4);
    let mut pkg_dirs = Vec::new();
    for entry in walker.into_iter() {
        let file = entry?;

        if file.file_name() == "defines" {
            let pkg_dir = file
                .path()
                .parent()
                .with_context(|| {
                    format!(
                        "The directory of defines file {} is root.",
                        file.path().display()
                    )
                })?
                .parent()
                .with_context(|| {
                    format!(
                        "The parent directory of defines file {} is root.",
                        file.path().display()
                    )
                })?;

            let spec_path = pkg_dir.join("spec");
            if !spec_path.is_file() {
                warn!(
                    "spec file not found in {} for {}. Skipping",
                    spec_path.display(),
                    file.path().display()
                );

                continue;
            }

            pkg_dirs.push((spec_path, file.path().to_path_buf()));
        }
    }

    Ok(pkg_dirs)
}
