//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.15

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "commits")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub pkg_name: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub pkg_version: String,
    pub spec_path: String,
    pub defines_path: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub tree: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub branch: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub commit_id: String,
    pub commit_time: DateTimeWithTimeZone,
    pub status: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
