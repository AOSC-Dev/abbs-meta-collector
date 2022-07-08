use std::{
    collections::HashMap,
    fmt::Write,
    fs,
    path::{Path, PathBuf},
};
use abbs_meta_apml::parse;
use abbs_meta_tree::Package;
use anyhow::Context;
use anyhow::Result;
use tracing::{info, log::warn};
use crate::db::commit::CommitDb;
use git2::Repository as Git2Repository;

pub fn scan_packages<P: AsRef<Path>>(abbs_path: P) -> Vec<Package> {
    let pkg_dirs = get_tree_dirs(abbs_path).unwrap();
    let mut pkgs = Vec::new();

    for (spec_path, defines_path) in pkg_dirs {
        let (context, error) = parse_spec_and_defines(&spec_path, &defines_path);
        if let Some(s) = error {
            warn!("{}", s);
        }

        match Package::from(&context, &spec_path) {
            Ok(pkg) => pkgs.push(pkg),
            Err(e) => warn!("{}", e),
        }
    }

    pkgs
}

pub async fn generate_changelog(
    pkg_name: &str,
    repo: &Git2Repository,
    commit_db: &CommitDb,
) -> Result<()> {
    let result = commit_db.get_package_commits(pkg_name).await.unwrap();

    for (commit_id, pkg_path) in result {
        let commit = repo.find_commit(commit_id)?;
        let spec_path = pkg_path + "/spec";

        let entry = match commit.tree()?.get_path(Path::new(&spec_path)) {
            Ok(entry) => entry,
            Err(e) => {
                warn!("cannot get entry {spec_path} at {commit:?} : {}",e.message());
                continue;
            }
        };

        let blob_oid = entry.id();
        let blob = repo.find_blob(blob_oid)?;
        let v = blob.content();
        let spec = std::str::from_utf8(v)?;

        let mut context = HashMap::new();
        parse(spec, &mut context).unwrap();

        let res = context.get("VER");
        info!("{commit:?} {:?}", res);
    }

    Ok(())
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
                    "spec file not found at {} for {}",
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
