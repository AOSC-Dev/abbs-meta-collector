use anyhow::Result;
use sea_orm::{
    sea_query::{IntoIden, OnConflict},
    ActiveModelBehavior, ActiveModelTrait, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait,
    ExecResult, Insert, InsertResult, IntoActiveModel, ModelTrait, QueryTrait, Schema, Statement,
    Value,
};
pub mod abbs;
pub mod commits;
pub mod entities;

#[async_trait::async_trait]
pub trait CreateTable: EntityTrait {
    async fn create_table(self, conn: &DatabaseConnection) -> Result<()> {
        let builder = conn.get_database_backend();
        let schema = Schema::new(builder);
        let mut commits_table = schema.create_table_from_entity(self);
        commits_table.if_not_exists();
        let state = builder.build(&commits_table);

        conn.execute(state).await?;

        Ok(())
    }
}
impl<E> CreateTable for E where E: EntityTrait {}

#[async_trait::async_trait]
pub trait InstertExt: ModelTrait {
    /// INSERT INTO TABLE VALUES (?....) ON CONFLICT UPDATE
    async fn replace<'a, A, C, CI, I1, I2>(
        self,
        db: &'a C,
        keys: I1,
        columns: I2,
    ) -> Result<InsertResult<A>, DbErr>
    where
        Self: IntoActiveModel<A>,
        C: ConnectionTrait,
        A: ActiveModelTrait<Entity = Self::Entity> + ActiveModelBehavior + Send + 'a,
        CI: IntoIden,
        I1: IntoIterator<Item = CI> + Send,
        I2: IntoIterator<Item = CI> + Send,
    {
        let mut insert = Insert::one(self.into_active_model());
        insert
            .query()
            .on_conflict(OnConflict::columns(keys).update_columns(columns).to_owned());
        insert.exec(db).await
    }

    /// INSERT OR IGNORE INTO TABLE VALUES (?....)
    async fn insert_or_ignore<'a, A, C>(self, db: &'a C) -> Result<InsertResult<A>, DbErr>
    where
        Self: IntoActiveModel<A>,
        C: ConnectionTrait,
        A: ActiveModelTrait<Entity = Self::Entity> + ActiveModelBehavior + Send + 'a,
    {
        let mut insert = Insert::one(self.into_active_model());
        insert
            .query()
            .on_conflict(OnConflict::new().do_nothing().to_owned());
        insert.exec(db).await
    }
}

impl<M> InstertExt for M where M: ModelTrait {}

async fn exec<I>(conn: &DatabaseConnection, sql: &str, values: I) -> Result<ExecResult>
where
    I: IntoIterator<Item = Value>,
{
    Ok(conn
        .execute(Statement::from_sql_and_values(
            conn.get_database_backend(),
            sql,
            values,
        ))
        .await?)
}

fn replace_many<A, M, I, CI, I1, I2>(models: I, keys: I1, columns: I2) -> Insert<A>
where
    A: ActiveModelTrait,
    M: IntoActiveModel<A>,
    I: IntoIterator<Item = M>,
    CI: IntoIden,
    I1: IntoIterator<Item = CI>,
    I2: IntoIterator<Item = CI>,
{
    let mut insert = Insert::many(models);
    insert
        .query()
        .on_conflict(OnConflict::columns(keys).update_columns(columns).to_owned());
    insert
}
