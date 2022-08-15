use super::entities::{
    fts_packages, package_changes, package_dependencies, package_duplicate, package_errors,
    package_spec, package_versions, packages, prelude::*, tree_branches, trees,
};
use super::{exec, replace_many, InstertExt};
use crate::db::CreateTable;
use crate::package::{Change, Context};
use crate::Config;
use abbs_meta_tree::Package;
use anyhow::{bail, Ok, Result};
use sea_orm::{entity::*, query::*};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, EntityTrait, QueryFilter};
use std::collections::{HashMap, HashSet};
use tracing::log::warn;

pub struct AbbsDb {
    conn: DatabaseConnection,
    tree: String,
    branch: String,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct PackageError {
    pub package: String,
    pub path: String,
    pub message: String,
    pub err_type: ErrorType,
}

impl AbbsDb {
    pub async fn open(config: &Config) -> Result<Self> {
        let Config {
            priority,
            abbs_db_path,
            branch,
            category,
            name,
            url,
            ..
        } = config;
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

        exec(&conn, "CREATE VIRTUAL TABLE IF NOT EXISTS fts_packages USING fts5(name, description, tokenize = porter)", []).await?;
        exec(
            &conn,
            "CREATE VIEW IF NOT EXISTS v_packages AS 
                SELECT p.name name, p.tree tree, 
                  t.category tree_category, 
                  pv.branch branch, p.category category, 
                  section, pkg_section, directory, description, version, 
                  ((CASE WHEN ifnull(epoch, '') = '' THEN '' 
                    ELSE epoch || ':' END) || version || 
                   (CASE WHEN ifnull(release, '') IN ('', '0') THEN '' 
                    ELSE '-' || release END)) full_version, 
                  pv.commit_time commit_time, pv.committer committer 
                FROM packages p 
                INNER JOIN trees t ON t.name=p.tree 
                LEFT JOIN package_versions pv 
                  ON pv.package=p.name AND pv.branch=t.mainbranch",
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

    pub async fn delete_package_errors(&self) -> Result<()> {
        Delete::many(PackageErrors)
            .filter(package_errors::Column::Tree.eq(self.tree.to_string()))
            .filter(package_errors::Column::Branch.eq(self.branch.to_string()))
            .exec(&self.conn)
            .await?;

        Ok(())
    }
    pub async fn add_package_errors(&self, errors: Vec<PackageError>) -> Result<()> {
        let iter = errors.into_iter().map(|e| package_errors::ActiveModel {
            package: Set(e.package),
            err_type: Set(e.err_type.to_string()),
            message: Set(e.message),
            path: Set(e.path),
            tree: Set(self.tree.clone()),
            branch: Set(self.branch.clone()),
            id: NotSet,
        });
        replace_many(iter).exec(&self.conn).await?;
        Ok(())
    }

    pub async fn add_package(
        &self,
        pkg: &Package,
        context: &Context,
        pkg_changes: Vec<Change>,
    ) -> Result<()> {
        let txn = self.conn.begin().await?;
        let db = &txn;

        if pkg_changes.is_empty() {
            bail!("cannot find changes of package, please update commit database")
        }
        let existing = Packages::find_by_id(pkg.name.clone()).one(db).await?;

        if let Some(existing) = existing {
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

            if existing.tree != self.tree {
                warn!(
                    "duplicate package \"{}\" found in different trees  {}/{}-{}/{} and {}/{}-{}/{}",
                    pkg.name,
                    existing.tree,
                    existing.category,
                    existing.section,
                    existing.directory,
                    self.tree,
                    pkg.category,
                    pkg.section,
                    pkg.directory
                );

                update_duplicate(pkg, &existing, &self.tree, db).await?;
            }

            if (&pkg.category, &pkg.section, &pkg.directory)
                != (&existing.category, &existing.section, &existing.directory)
            {
                warn!(
                    "duplicate package \"{}\" found in {}-{}/{} and {}-{}/{}",
                    pkg.name,
                    existing.category,
                    existing.section,
                    existing.directory,
                    pkg.category,
                    pkg.section,
                    pkg.directory
                );

                update_duplicate(pkg, &existing, &self.tree, db).await?;
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

        let iter = context.iter().map(|(k, v)| {
            package_spec::Model {
                package: pkg.name.clone(),
                key: k.clone(),
                value: v.clone(),
            }
            .into_active_model()
        });
        replace_many(iter).exec(db).await?;

        PackageDependencies::delete_many()
            .filter(package_dependencies::Column::Package.eq(pkg.name.clone()))
            .exec(db)
            .await?;

        let pkg_name = &pkg.name;
        type PkgDep = HashMap<String, Vec<(String, Option<String>, Option<String>)>>;
        async fn helper(
            pkgdep: &PkgDep,
            relationship: &str,
            pkg_name: &str,
            db: &impl ConnectionTrait,
        ) -> Result<()> {
            for (architecture, v) in pkgdep.iter() {
                let architecture = if architecture == "default" {
                    ""
                } else {
                    architecture
                };
                for (dependency, relop, version) in v.clone() {
                    package_dependencies::Model {
                        package: pkg_name.to_string(),
                        dependency,
                        relop,
                        version,
                        architecture: architecture.to_string(),
                        relationship: relationship.to_string(),
                    }
                    .replace(db)
                    .await?;
                }
            }
            Ok(())
        }

        helper(&pkg.dependencies, "PKGDEP", pkg_name, db).await?;
        helper(&pkg.build_dependencies, "BUILDDEP", pkg_name, db).await?;
        helper(&pkg.package_suggests, "PKGSUG", pkg_name, db).await?;
        helper(&pkg.package_provides, "PKGPROV", pkg_name, db).await?;
        helper(&pkg.package_recommands, "PKGRECOM", pkg_name, db).await?;
        helper(&pkg.package_replaces, "PKGREP", pkg_name, db).await?;
        helper(&pkg.package_breaks, "PKGBREAK", pkg_name, db).await?;
        helper(&pkg.package_configs, "PKGCONFIG", pkg_name, db).await?;

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

        Ok(())
    }

    pub async fn delete_package_many(
        &self,
        pkg_names: impl Iterator<Item = impl AsRef<str>>,
    ) -> Result<()> {
        for pkg_name in pkg_names {
            self.delete_package(pkg_name.as_ref()).await?;
        }

        Ok(())
    }
}
