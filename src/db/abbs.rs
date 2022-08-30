use super::commits::{Change, CommitDb};
use super::entities::{
    fts_packages, package_changes, package_dependencies, package_duplicate, package_errors,
    package_spec, package_testing, package_versions, packages, prelude::*, tree_branches, trees,
};
use super::{exec, replace_many, InstertExt};
use crate::config::{Global, Repo};
use crate::db::CreateTable;
use crate::git::Repository;
use crate::package::Meta;
use crate::skip_none;
use abbs_meta_tree::Package;
use anyhow::{bail, Result};
use git2::Oid;
use itertools::Itertools;
use sea_orm::{entity::*, query::*};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::info;
use tracing::log::warn;

pub struct AbbsDb {
    conn: DatabaseConnection,
    tree: String,
    branch: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ErrorType {
    Parse,
    Package,
}

impl ToString for ErrorType {
    fn to_string(&self) -> String {
        match self {
            Self::Parse => "parse",
            Self::Package => "package",
        }
        .to_string()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackageError {
    pub package: String,
    pub path: String,
    pub message: String,
    pub err_type: ErrorType,
    pub line: Option<u32>,
    pub col: Option<u32>,
}

impl AbbsDb {
    pub async fn open(global_config: &Global, repo_config: &Repo) -> Result<Self> {
        let abbs_db_path = &global_config.abbs_db_path;
        let Repo {
            branch,
            priority,
            category,
            name,
            url,
            ..
        } = repo_config;

        let conn = Database::connect(format!("sqlite://{abbs_db_path}?mode=rwc")).await?;

        PackageDependencies.create_table(&conn).await?;
        PackageDuplicate.create_table(&conn).await?;
        PackageSpec.create_table(&conn).await?;
        PackageVersions.create_table(&conn).await?;
        Packages.create_table(&conn).await?;
        TreeBranches.create_table(&conn).await?;
        Trees.create_table(&conn).await?;
        PackageChanges.create_table(&conn).await?;
        PackageErrors.create_table(&conn).await?;
        PackageTesting.create_table(&conn).await?;

        exec(
            &conn,
            "CREATE VIRTUAL TABLE IF NOT EXISTS fts_packages USING fts5(name, description, tokenize = porter)",
            [],
        )
        .await?;
        exec(
            &conn,
            "
            CREATE VIEW IF NOT EXISTS v_packages AS
            SELECT
                p.name name,
                p.tree tree,
                t.category tree_category,
                pv.branch branch,
                p.category category,
                section,
                pkg_section,
                directory,
                description,
                version,
                spec_path,
                (
                    (
                        CASE
                            WHEN ifnull(epoch, '') = '' THEN ''
                            ELSE epoch || ':'
                        END
                    ) || version || (
                        CASE
                            WHEN ifnull(release, '') IN ('', '0') THEN ''
                            ELSE '-' || release
                        END
                    )
                ) full_version,
                pv.commit_time commit_time,
                pv.committer committer
            FROM
                packages p
                INNER JOIN trees t ON t.name = p.tree
                LEFT JOIN package_versions pv ON pv.package = p.name
                AND pv.branch = t.mainbranch",
            [],
        )
        .await?;

        trees::Model {
            tid: *priority,
            name: name.into(),
            category: category.into(),
            url: url.into(),
            mainbranch: branch.into(),
        }
        .replace(&conn)
        .await?;

        trees::Model {
            tid: *priority,
            name: name.into(),
            category: category.into(),
            url: url.into(),
            mainbranch: branch.into(),
        }
        .replace(&conn)
        .await?;

        tree_branches::Model {
            name: format!("{name}/{branch}"),
            tree: name.into(),
            branch: branch.into(),
            priority: Some(*priority),
        }
        .replace(&conn)
        .await?;

        Ok(Self {
            conn,
            tree: name.clone(),
            branch: branch.clone(),
        })
    }

    pub async fn add_package(&self, pkg_meta: Meta, pkg_changes: Vec<Change>) -> Result<()> {
        let (pkg, context, errors) = pkg_meta;
        let txn = self.conn.begin().await?;
        let db = &txn;

        if pkg_changes.is_empty() {
            bail!("cannot find changes of package, please update commit database")
        }
        let existing = Packages::find_by_id(pkg.name.clone()).one(db).await?;

        if let Some(existing) = existing {
            let name = &pkg.name;
            let existing_tree = &existing.tree;
            let existing_category = &existing.category;
            let existing_section = &existing.section;
            let existing_directory = &existing.directory;
            let tree = &self.tree;
            let category = &pkg.category;
            let section = &pkg.section;
            let directory = &pkg.directory;

            if existing.tree != self.tree {
                warn!(
                    "duplicate package \"{name}\" found in different trees {existing_tree}/{existing_category}-{existing_section}/{existing_directory} and {tree}/{category}-{section}/{directory}",
                );
                update_duplicate(&pkg, &existing, &self.tree, db).await?;
            }

            if (&pkg.category, &pkg.section, &pkg.directory)
                != (&existing.category, &existing.section, &existing.directory)
            {
                warn!(
                    "duplicate package \"{name}\" found in {existing_category}-{existing_section}/{existing_directory} and {category}-{section}/{directory}",
                );
                update_duplicate(&pkg, &existing, &self.tree, db).await?;
            }
        }

        packages::Model {
            name: pkg.name.clone(),
            tree: self.tree.clone(),
            category: pkg.category.clone(),
            section: pkg.section.clone(),
            pkg_section: pkg.pkg_section.clone(),
            directory: pkg.directory.clone(),
            description: pkg.description.clone(),
            spec_path: pkg.spec_path.clone(),
        }
        .replace(&txn)
        .await?;

        let res = FtsPackages::find()
            .filter(fts_packages::Column::Name.eq(pkg.name.clone()))
            .one(db)
            .await?;

        let model = fts_packages::Model {
            name: pkg.name.clone(),
            description: pkg.description.clone(),
        };

        if let Some(res) = res {
            if res.description != pkg.description {
                res.delete(db).await?;
                model.replace(db).await?;
            }
        } else {
            model.replace(db).await?;
        }

        let first = pkg_changes[0].clone();
        let changes_iter = pkg_changes.into_iter().map(|change| {
            package_changes::Model {
                package: change.pkg_name,
                githash: change.githash,
                version: change.version,
                branch: change.branch,
                urgency: change.urgency,
                message: change.message,
                maintainer_name: change.maintainer_name,
                maintainer_email: change.maintainer_email,
                timestamp: change.timestamp,
                tree: change.tree,
            }
            .into_active_model()
        });
        replace_many(changes_iter).exec(db).await?;

        package_versions::Model {
            package: pkg.name.clone(),
            branch: self.branch.clone(),
            architecture: "".to_string(),
            version: pkg.version.clone(),
            release: Some(pkg.release).filter(|x| *x != 0).map(|x| x.to_string()),
            epoch: Some(pkg.epoch).filter(|x| *x != 0).map(|x| x.to_string()),
            commit_time: first.timestamp,
            committer: format!(
                "{name} <{email}>",
                name = first.maintainer_name,
                email = first.maintainer_email
            ),
            githash: first.githash.clone(),
        }
        .replace(db)
        .await?;

        PackageSpec::delete_many()
            .filter(package_spec::Column::Package.eq(pkg.name.clone()))
            .exec(db)
            .await?;

        let iter = context.into_iter().map(|(k, v)| {
            package_spec::Model {
                package: pkg.name.clone(),
                key: k,
                value: v,
            }
            .into_active_model()
        });
        replace_many(iter).exec(db).await?;

        PackageDependencies::delete_many()
            .filter(package_dependencies::Column::Package.eq(pkg.name.clone()))
            .exec(db)
            .await?;

        let pkg_name = &pkg.name;

        add_dependencies(pkg.dependencies, "PKGDEP", pkg_name, db).await?;
        add_dependencies(pkg.build_dependencies, "BUILDDEP", pkg_name, db).await?;
        add_dependencies(pkg.package_suggests, "PKGSUG", pkg_name, db).await?;
        add_dependencies(pkg.package_provides, "PKGPROV", pkg_name, db).await?;
        add_dependencies(pkg.package_recommands, "PKGRECOM", pkg_name, db).await?;
        add_dependencies(pkg.package_replaces, "PKGREP", pkg_name, db).await?;
        add_dependencies(pkg.package_breaks, "PKGBREAK", pkg_name, db).await?;
        add_dependencies(pkg.package_configs, "PKGCONFIG", pkg_name, db).await?;

        // package_errors
        if !errors.is_empty() {
            let iter = errors.into_iter().map(|e| package_errors::ActiveModel {
                package: Set(e.package),
                err_type: Set(e.err_type.to_string()),
                message: Set(e.message),
                path: Set(e.path),
                tree: Set(self.tree.clone()),
                branch: Set(self.branch.clone()),
                line: Set(e.line),
                col: Set(e.col),
                id: NotSet,
            });
            replace_many(iter).exec(db).await?;
        }

        txn.commit().await?;
        Ok(())
    }

    pub async fn get_packages_name(&self) -> Result<HashSet<String>> {
        let res = Packages::find()
            .filter(packages::Column::Tree.eq(self.tree.clone()))
            .all(&self.conn)
            .await?;
        Ok(res.into_iter().map(|model| model.name).collect())
    }

    pub async fn delete_package(&self, pkg_name: impl AsRef<str>) -> Result<()> {
        let pkg_name = pkg_name.as_ref();
        let db = &self.conn;

        Delete::many(PackageVersions)
            .filter(package_versions::Column::Package.eq(pkg_name.to_string()))
            .filter(package_versions::Column::Branch.eq(self.branch.clone()))
            .exec(db)
            .await?;

        Delete::many(PackageSpec)
            .filter(package_spec::Column::Package.eq(pkg_name.to_string()))
            .exec(db)
            .await?;

        Delete::many(PackageDependencies)
            .filter(package_dependencies::Column::Package.eq(pkg_name.to_string()))
            .exec(db)
            .await?;

        Delete::many(Packages)
            .filter(packages::Column::Name.eq(pkg_name.to_string()))
            .filter(packages::Column::Tree.eq(self.tree.clone()))
            .exec(db)
            .await?;

        Delete::many(FtsPackages)
            .filter(fts_packages::Column::Name.eq(pkg_name.to_string()))
            .exec(db)
            .await?;

        Delete::many(PackageErrors)
            .filter(package_errors::Column::Package.eq(pkg_name.to_string()))
            .filter(package_errors::Column::Tree.eq(self.tree.to_string()))
            .filter(package_errors::Column::Branch.eq(self.branch.to_string()))
            .exec(db)
            .await?;

        Delete::many(PackageTesting)
            .filter(package_testing::Column::Package.eq(pkg_name.to_string()))
            .filter(package_testing::Column::Tree.eq(self.tree.to_string()))
            .filter(package_testing::Column::Branch.eq(self.branch.to_string()))
            .exec(db)
            .await?;

        Ok(())
    }

    pub async fn update_testing_branch(
        &self,
        commit_db: &CommitDb,
        repo: &Repository,
        exculde: &HashSet<String>,
    ) -> Result<()> {
        let result = commit_db.update_package_testing(repo, exculde).await?;

        let main = scan_branch(repo, repo.get_repo_branch(), Some(1000))?;
        let mut outdated_branches = vec![];

        for (branch, info) in result {
            info!("scan testing branch {branch}");
            let testing = scan_branch(repo, &branch, None)?;
            let last = testing
                .iter()
                .filter_map(|(oid, order)| {
                    main.get(oid)
                        .map(|main_branch_order| (main_branch_order, order))
                })
                .max_by_key(|x| x.0);
            let (_, last) = if let Some(last) = last {
                last
            } else {
                outdated_branches.push(branch.to_string());
                continue;
            };

            for info in info {
                let new_order = skip_none!(testing.get(&info.commit_id));

                let db_order = PackageTesting::find()
                    .filter(package_testing::Column::Package.eq(info.pkg_name.clone()))
                    .filter(package_testing::Column::Tree.eq(repo.tree.clone()))
                    .filter(package_testing::Column::Branch.eq(branch.clone()))
                    .one(&self.conn)
                    .await?
                    .and_then(|current| testing.get(&Oid::from_str(&current.commit).ok()?))
                    .unwrap_or(&10_0000);

                if (new_order < db_order) & (new_order <= last) {
                    package_testing::Model {
                        spec_path: info.spec_path,
                        package: info.pkg_name,
                        version: info.pkg_version,
                        defines_path: info.defines_path,
                        branch: branch.clone(),
                        tree: repo.tree.clone(),
                        commit: info.commit_id.to_string(),
                    }
                    .replace(&self.conn)
                    .await?;
                } else if (new_order > last) & (db_order > last) {
                    PackageTesting::delete_by_id((
                        info.pkg_name,
                        repo.tree.clone(),
                        branch.clone(),
                    ))
                    .exec(&self.conn)
                    .await?;
                }
            }
        }

        // delete unused branch
        let current_branches_name = repo
            .get_git2repo()
            .branches(None)?
            .filter_map(|b| Some(b.ok()?.0.name().ok()??.to_string()))
            .collect_vec();
        PackageTesting::delete_many()
            .filter(package_testing::Column::Tree.eq(repo.tree.clone()))
            .filter(package_testing::Column::Branch.is_not_in(current_branches_name))
            .exec(&self.conn)
            .await?;
        PackageTesting::delete_many()
            .filter(package_testing::Column::Tree.eq(repo.tree.clone()))
            .filter(package_testing::Column::Branch.is_in(outdated_branches))
            .exec(&self.conn)
            .await?;

        Ok(())
    }

    pub async fn delete_packages(
        &self,
        pkg_names: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<()> {
        for pkg_name in pkg_names {
            self.delete_package(pkg_name.as_ref()).await?;
        }

        Ok(())
    }
}

fn scan_branch(
    repo: &Repository,
    branch_name: &str,
    take: Option<usize>,
) -> Result<HashMap<Oid, usize>> {
    use anyhow::Context;
    let repo = repo.get_git2repo();

    let branch = repo
        .find_branch(branch_name, git2::BranchType::Remote)
        .or_else(|_| repo.find_branch(branch_name, git2::BranchType::Local))?;
    let mut revwalk = repo.revwalk()?;
    revwalk.push(
        branch
            .get()
            .target()
            .with_context(|| format!("failed to get commit of branch {}", branch_name))?,
    )?;
    Ok(revwalk
        .take(take.unwrap_or(100000000))
        .enumerate()
        .filter_map(|(i, x)| Some((x.ok()?, i)))
        .collect())
}

async fn update_duplicate(
    pkg: &Package,
    existing: &packages::Model,
    tree: &str,
    db: &impl ConnectionTrait,
) -> Result<()> {
    package_duplicate::Model {
        package: pkg.name.clone(),
        tree: tree.to_string(),
        category: pkg.category.clone(),
        section: pkg.section.clone(),
        directory: pkg.directory.clone(),
    }
    .insert_or_ignore(db)
    .await?;

    package_duplicate::Model {
        package: pkg.name.clone(),
        tree: existing.tree.clone(),
        category: existing.category.clone(),
        section: existing.section.clone(),
        directory: existing.directory.clone(),
    }
    .insert_or_ignore(db)
    .await?;

    Ok(())
}

type PkgDep = HashMap<String, Vec<(String, Option<String>, Option<String>)>>;
async fn add_dependencies(
    pkgdep: PkgDep,
    relationship: &str,
    pkg_name: &str,
    db: &impl ConnectionTrait,
) -> Result<()> {
    for (architecture, v) in pkgdep {
        let architecture = (architecture == "default")
            .then_some("")
            .unwrap_or(architecture.as_str());

        for (dependency, relop, version) in v.clone() {
            package_dependencies::Model {
                package: pkg_name.into(),
                dependency,
                relop,
                version,
                architecture: architecture.into(),
                relationship: relationship.into(),
            }
            .replace(db)
            .await?;
        }
    }
    Ok(())
}
