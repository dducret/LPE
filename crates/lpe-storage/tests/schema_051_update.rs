use std::{env, str::FromStr};

use anyhow::{Context, Result};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Row,
};
use uuid::Uuid;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const PREFLIGHT_SQL: &str =
    include_str!("../sql/updates/0.5.0-sql-v1-to-0.5.1-sql-preflight.sql");
const CACHE_FIDELITY_UPDATE_SQL: &str =
    include_str!("../sql/updates/0.5.0-sql-v1-outlook-cache-fidelity.sql");
const VERSION_UPDATE_SQL: &str =
    include_str!("../sql/updates/0.5.0-sql-v1-to-0.5.1-sql.sql");

#[tokio::test]
async fn schema_051_update_is_transactional_idempotent_and_version_bounded() -> Result<()> {
    let Some(database_url) = env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("skipping 0.5.1 schema update test; TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let schema_name = format!("lpe_051_update_{}", Uuid::new_v4().simple());
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&database_url)?)
        .await
        .context("connect to TEST_DATABASE_URL for the 0.5.1 schema update")?;

    let result = run_update_scenarios(&pool, &schema_name).await;
    if result.is_err() {
        let _ = sqlx::query("ROLLBACK").execute(&pool).await;
    }
    let cleanup = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        .execute(&pool)
        .await
        .with_context(|| format!("drop isolated update test schema {schema_name}"));
    pool.close().await;

    cleanup?;
    result
}

async fn run_update_scenarios(pool: &PgPool, schema_name: &str) -> Result<()> {
    recreate_source_schema(pool, schema_name, "0.5.0-sql-v1").await?;
    let preflight_sql = sql_for_schema(PREFLIGHT_SQL, schema_name)?;
    let cache_fidelity_update_sql = sql_for_schema(CACHE_FIDELITY_UPDATE_SQL, schema_name)?;
    let version_update_sql = sql_for_schema(VERSION_UPDATE_SQL, schema_name)?;

    execute_update(pool, &preflight_sql)
        .await
        .context("validate the late canonical 0.5.0 source shape")?;
    assert_schema_version(pool, schema_name, "0.5.0-sql-v1").await?;
    execute_update(pool, &cache_fidelity_update_sql)
        .await
        .context("apply the Outlook cache fidelity update")?;
    execute_update(pool, &version_update_sql)
        .await
        .context("migrate 0.5.0-sql-v1 to 0.5.1-sql")?;
    assert_schema_version(pool, schema_name, "0.5.1-sql").await?;

    execute_update(pool, &version_update_sql)
        .await
        .context("apply the 0.5.1 schema update a second time")?;
    assert_schema_version(pool, schema_name, "0.5.1-sql").await?;

    recreate_source_schema(pool, schema_name, "0.5.0-sql-v1").await?;
    sqlx::query(&format!(
        "ALTER TABLE {schema_name}.mapi_object_identities DROP COLUMN predecessor_change_list"
    ))
    .execute(pool)
    .await
    .context("create the unsupported early 0.5.0 physical shape")?;
    let error = execute_update(pool, &preflight_sql)
        .await
        .expect_err("an early physical 0.5.0 shape must be rejected before mutation");
    anyhow::ensure!(
        format!("{error:#}").contains("physical shape"),
        "early 0.5.0 rejection must identify the unsupported physical shape: {error:#}"
    );
    assert_schema_version(pool, schema_name, "0.5.0-sql-v1").await?;
    let error = execute_update(pool, &version_update_sql)
        .await
        .expect_err("the label transition must independently reject an incomplete source shape");
    anyhow::ensure!(
        format!("{error:#}").contains("target physical shape"),
        "the label transition must identify the incomplete target shape: {error:#}"
    );
    assert_schema_version(pool, schema_name, "0.5.0-sql-v1").await?;

    recreate_source_schema(pool, schema_name, "0.4.9-sql-v1").await?;
    let error = execute_update(pool, &preflight_sql)
        .await
        .expect_err("pre-0.5 schema must be rejected");
    let error_text = format!("{error:#}");
    anyhow::ensure!(
        error_text.contains("unsupported LPE schema version"),
        "pre-0.5 rejection must identify the unsupported version: {error:#}"
    );
    assert_schema_version(pool, schema_name, "0.4.9-sql-v1").await?;

    Ok(())
}

fn sql_for_schema(sql: &str, schema_name: &str) -> Result<String> {
    let rewritten = sql
        .replace("public.", &format!("{schema_name}."))
        .replace("'public'", &format!("'{schema_name}'"))
        .replace(
            "SET LOCAL search_path = pg_catalog, public;",
            &format!("SET LOCAL search_path = pg_catalog, {schema_name};"),
        );

    anyhow::ensure!(
        !rewritten.contains("public.")
            && !rewritten.contains("'public'")
            && !rewritten.contains("SET LOCAL search_path = pg_catalog, public;"),
        "the isolated migration rewrite left a public-schema qualification"
    );
    Ok(rewritten)
}

async fn recreate_source_schema(pool: &PgPool, schema_name: &str, version: &str) -> Result<()> {
    sqlx::raw_sql(&format!(
        "DROP SCHEMA IF EXISTS {schema_name} CASCADE; CREATE SCHEMA {schema_name};"
    ))
    .execute(pool)
    .await?;
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public")
        .execute(pool)
        .await?;
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
        .execute(pool)
        .await?;

    let isolated_schema_sql = SCHEMA_SQL.replacen(
        "BEGIN;",
        &format!("BEGIN;\nSET LOCAL search_path TO {schema_name}, public;"),
        1,
    );
    sqlx::raw_sql(&isolated_schema_sql)
        .execute(pool)
        .await
        .context("apply the canonical schema as the realistic migration fixture")?;
    sqlx::raw_sql(&format!(
        r#"
        ALTER TABLE {schema_name}.schema_metadata
            DROP CONSTRAINT schema_metadata_schema_version_check;
        UPDATE {schema_name}.schema_metadata
        SET schema_version = '{version}'
        WHERE singleton = TRUE;
        ALTER TABLE {schema_name}.schema_metadata
            ADD CONSTRAINT schema_metadata_schema_version_check
            CHECK (schema_version = '{version}');
        "#
    ))
    .execute(pool)
    .await
    .with_context(|| format!("set isolated schema source version to {version}"))?;
    Ok(())
}

async fn execute_update(pool: &PgPool, update_sql: &str) -> Result<()> {
    let mut connection = pool
        .acquire()
        .await
        .context("acquire 0.5.1 schema update connection")?;
    match sqlx::raw_sql(update_sql).execute(&mut *connection).await {
        Ok(_) => Ok(()),
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error).context("execute 0.5.1 schema update")
        }
    }
}

async fn assert_schema_version(pool: &PgPool, schema_name: &str, expected: &str) -> Result<()> {
    let row = sqlx::query(&format!(
        r#"
        SELECT metadata.schema_version, pg_get_constraintdef(constraint_row.oid) AS constraint_def
        FROM {schema_name}.schema_metadata metadata
        JOIN pg_constraint constraint_row
          ON constraint_row.conrelid = '{schema_name}.schema_metadata'::regclass
         AND constraint_row.conname = 'schema_metadata_schema_version_check'
        WHERE metadata.singleton = TRUE
        "#
    ))
    .fetch_one(pool)
    .await
    .context("read migrated schema metadata")?;
    let version = row.get::<String, _>("schema_version");
    let constraint = row.get::<String, _>("constraint_def");
    anyhow::ensure!(version == expected, "expected {expected}, found {version}");
    anyhow::ensure!(
        constraint.contains(expected),
        "schema version constraint does not require {expected}: {constraint}"
    );
    Ok(())
}
