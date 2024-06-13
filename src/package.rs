use crate::db::abbs::ErrorType;
use crate::db::abbs::PackageError;
use crate::git::Repository;
use crate::skip_none;
use abbs_meta_apml::parse;
use abbs_meta_tree::Package;
use anyhow::Context as AnyhowContext;
use anyhow::Result;
use git2::Oid;
use git2::TreeWalkResult;
use itertools::Itertools;
use std::ffi::OsStr;
use std::path::Path;
use std::{collections::HashMap, path::PathBuf};
pub type Context = HashMap<String, String>;
pub type Meta = (Package, Context, Vec<PackageError>);

pub fn scan_packages(
    repo: &Repository,
    commit: Oid,
    pkg_dirs: Vec<(&PathBuf, &PathBuf)>,
) -> Vec<Meta> {
    pkg_dirs
        .iter()
        .filter_map(|(spec, defines)| {
            let (pkg, errors) = scan_package(repo, commit, spec, defines);
            let pkg = pkg?;
            Some((pkg.0, pkg.1, errors))
        })
        .collect_vec()
}

#[inline(always)]
pub fn scan_package(
    repo: &Repository,
    commit: Oid,
    spec_path: &PathBuf,
    defines_path: &PathBuf,
) -> (Option<(Package, Context)>, Vec<PackageError>) {
    macro_rules! skip_none {
        ($res:expr) => {
            match $res {
                Some(val) => val,
                None => return (None, vec![]),
            }
        };
    }

    let (context, mut errors) = skip_none!(parse_spec_and_defines(
        repo,
        commit,
        spec_path,
        defines_path,
    ));

    match Package::from(&context, spec_path) {
        Ok(pkg) => (Some((pkg, context)), errors),
        Err(e) => {
            let pkg_name = skip_none!(skip_none!(defines_path.iter().nth_back(2)).to_str());

            // extra-doc/jade/autobuild/defines -> extra-doc/jade
            let path = skip_none!(skip_none!(defines_path.ancestors().nth(2)).to_str()).to_string();
            errors.push(PackageError {
                package: pkg_name.to_string(),
                path,
                message: e.to_string(),
                err_type: ErrorType::Package,
                line: None,
                col: None,
            });
            (None, errors)
        }
    }
}

fn parse_spec_and_defines(
    repo: &Repository,
    commit: Oid,
    spec_path: &PathBuf,
    defines_path: &PathBuf,
) -> Option<(Context, Vec<PackageError>)> {
    let spec = repo.read_file(spec_path, commit).ok()?;
    let defines = repo.read_file(defines_path, commit).ok()?;
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
                line: Some(e.line as i32),
                col: Some(e.col as i32),
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
                line: Some(e.line as i32),
                col: Some(e.col as i32),
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

pub fn spec_path_to_defines_path(
    repo: &Repository,
    commit: Oid,
    spec_path: &Path,
) -> Result<Vec<PathBuf>> {
    let tree = repo.find_commit(commit)?.tree()?;

    let walk = |path| -> Result<_> {
        let entry = tree.get_path(path)?;
        let pkg_tree = repo.get_git2repo().find_tree(entry.id())?;
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
        Ok(dirs)
    };

    let pkg_path = spec_path
        .parent()
        .with_context(|| format!("The path {} doesn't have parent", spec_path.display()))?;
    let res = walk(pkg_path)?
        .into_iter()
        .filter(|path| path.file_name() == Some(OsStr::new("defines")))
        .collect_vec();
    Ok(res)
}

pub fn defines_path_to_spec_path(defines_path: &Path) -> Result<PathBuf> {
    let mut pkg_dir = defines_path
        .parent()
        .with_context(|| {
            format!(
                "The directory of defines file {} is root.",
                defines_path.display()
            )
        })?
        .parent()
        .with_context(|| {
            format!(
                "The parent directory of defines file {} is root.",
                defines_path.display()
            )
        })?
        .to_path_buf();
    pkg_dir.push("spec");
    Ok(pkg_dir)
}

pub fn path_to_defines_path(repo: &Repository, commit: Oid, path: &Path) -> Result<Vec<PathBuf>> {
    let file_name = path
        .file_name()
        .with_context(|| format!("failed to convert {} to str", path.display()))?
        .to_str()
        .with_context(|| format!("failed to convert {} to str", path.display()))?;

    match file_name {
        "defines" => Ok(vec![path.to_path_buf()]),
        "spec" => Ok(spec_path_to_defines_path(repo, commit, path)?),
        _ => {
            let tree = repo.find_commit(commit)?.tree()?;
            path.ancestors()
                .find_map(|path| {
                    let mut path = path.to_path_buf();
                    path.push(Path::new("defines"));
                    tree.get_path(&path).ok().map(|_| vec![path.to_path_buf()])
                })
                .with_context(|| {
                    format!(
                        "failed to find defines path at the ancestors of {}",
                        path.display()
                    )
                })
        }
    }
}
