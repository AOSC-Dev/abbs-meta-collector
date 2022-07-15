use anyhow::Result;

use sea_orm::{
    sea_query::OnConflict, ActiveModelTrait, ConnectionTrait, DatabaseConnection, EntityTrait,
    ExecResult, Insert, IntoActiveModel, QueryTrait, Schema, Statement, Value,
};
pub mod abbs;
pub mod commits;
pub mod entities;

async fn create_table(conn: &DatabaseConnection, entity: impl EntityTrait) -> Result<()> {
    let builder = conn.get_database_backend();
    let schema = Schema::new(builder);

    let mut commits_table = schema.create_table_from_entity(entity);
    commits_table.if_not_exists();

    let state = builder.build(&commits_table);

    conn.execute(state).await?;

    Ok(())
}

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

fn replace_many<A, M, I>(models: I) -> Insert<A>
where
    A: ActiveModelTrait,
    M: IntoActiveModel<A>,
    I: IntoIterator<Item = M>,
{
    let mut insert = Insert::many(models);
    insert.query().replace();
    insert
}

/// REPLACE INTO TABLE VALUES (?....)
fn replace<A>(model: A) -> Insert<A>
where
    A: ActiveModelTrait,
{
    let mut insert = Insert::one(model);
    insert.query().replace();
    insert
}

/// INSERT OR IGNORE INTO TABLE VALUES (?....)
fn insert_or_ignore<A>(model: A) -> Insert<A>
where
    A: ActiveModelTrait,
{
    let mut insert = Insert::one(model);
    insert
        .query()
        .on_conflict(OnConflict::new().do_nothing().to_owned());
    insert
}
