use anyhow::{bail, Context, Result};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Pool, Postgres,
};

use crate::EXPECTED_SCHEMA_VERSION;

#[derive(Clone)]
pub struct Storage {
    pub(crate) pool: Pool<Postgres>,
    database_url: Option<String>,
}

impl Storage {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            database_url: None,
        }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        let connect_options = database_url
            .parse::<PgConnectOptions>()?
            .options([("search_path", "public")]);
        let pool = PgPoolOptions::new().connect_with(connect_options).await?;
        let storage = Self {
            pool,
            database_url: Some(database_url.to_string()),
        };
        storage.assert_schema_version().await?;
        Ok(storage)
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    pub fn database_url(&self) -> Option<&str> {
        self.database_url.as_deref()
    }

    async fn assert_schema_version(&self) -> Result<()> {
        let schema_version = sqlx::query_scalar::<_, String>(
            r#"
            SELECT schema_version
            FROM public.schema_metadata
            WHERE singleton = TRUE
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context(
                "database schema is not initialized for LPE; recreate the database and apply crates/lpe-storage/sql/schema.sql",
        )?;

        if schema_version != EXPECTED_SCHEMA_VERSION {
            bail!(
                "unsupported database schema version {schema_version}; expected {EXPECTED_SCHEMA_VERSION}. Initialize a fresh database from crates/lpe-storage/sql/schema.sql"
            );
        }

        self.assert_required_schema_objects("public").await?;

        Ok(())
    }

    async fn assert_required_schema_objects(&self, schema_name: &str) -> Result<()> {
        for table in [
            "accounts",
            "calendar_events",
            "mapi_calendar_event_identity_moves",
            "mapi_object_identities",
            "mapi_named_properties",
            "mapi_custom_property_values",
            "mapi_folder_profile_property_values",
            "mapi_associated_config_messages",
            "mapi_profile_settings",
        ] {
            let present = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = $1
                      AND table_name = $2
                      AND table_type = 'BASE TABLE'
                )
                "#,
            )
            .bind(&schema_name)
            .bind(table)
            .fetch_one(&self.pool)
            .await
            .with_context(|| format!("unable to inspect required table {schema_name}.{table}"))?;

            if !present {
                bail!(
                    "required table {schema_name}.{table} is missing; LPE 0.5.0 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
                );
            }
        }

        let mut invalid_columns = Vec::new();
        for (column, data_type) in [
            ("mapi_change_number", "bigint"),
            ("predecessor_change_list", "bytea"),
        ] {
            let present = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = $1
                      AND table_name = 'mapi_object_identities'
                      AND column_name = $2
                      AND data_type = $3
                      AND is_nullable = 'NO'
                )
                "#,
            )
            .bind(&schema_name)
            .bind(column)
            .bind(data_type)
            .fetch_one(&self.pool)
            .await
            .with_context(|| {
                format!(
                    "unable to inspect required column {schema_name}.mapi_object_identities.{column}"
                )
            })?;

            if !present {
                invalid_columns.push(format!("{column} {data_type} NOT NULL"));
            }
        }

        if !invalid_columns.is_empty() {
            bail!(
                "required column shapes {} are missing or incompatible in {schema_name}.mapi_object_identities; LPE 0.5.0 requires an empty database initialized from crates/lpe-storage/sql/schema.sql",
                invalid_columns.join(", ")
            );
        }

        let mapi_change_key_constraint_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM pg_constraint constraint_row
            JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
            JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
            WHERE namespace_row.nspname = $1
              AND constraint_row.contype = 'c'
              AND (
                    (
                        table_row.relname = 'mapi_object_identities'
                        AND pg_get_constraintdef(constraint_row.oid)
                            LIKE '%octet_length(change_key) >= 17%'
                        AND pg_get_constraintdef(constraint_row.oid)
                            LIKE '%octet_length(change_key) <= 24%'
                    )
                    OR (
                        table_row.relname = 'mapi_calendar_event_identity_moves'
                        AND pg_get_constraintdef(constraint_row.oid)
                            LIKE '%octet_length(old_change_key) >= 17%'
                        AND pg_get_constraintdef(constraint_row.oid)
                            LIKE '%octet_length(old_change_key) <= 24%'
                    )
                    OR (
                        table_row.relname = 'mapi_calendar_event_identity_moves'
                        AND pg_get_constraintdef(constraint_row.oid)
                            LIKE '%octet_length(new_change_key) >= 17%'
                        AND pg_get_constraintdef(constraint_row.oid)
                            LIKE '%octet_length(new_change_key) <= 24%'
                    )
              )
            "#,
        )
        .bind(schema_name)
        .fetch_one(&self.pool)
        .await
        .with_context(|| {
            format!("unable to inspect MAPI ChangeKey XID constraints in schema {schema_name}")
        })?;
        if mapi_change_key_constraint_count != 3 {
            bail!(
                "required 17-24-byte MAPI ChangeKey XID constraints are missing or incompatible in {schema_name}; LPE 0.5.0 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        let mut invalid_calendar_lifecycle_columns = Vec::new();
        for (column, data_type, is_nullable) in [
            ("lifecycle_state", "text", "NO"),
            ("deleted_at", "timestamp with time zone", "YES"),
        ] {
            let present = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = $1
                      AND table_name = 'calendar_events'
                      AND column_name = $2
                      AND data_type = $3
                      AND is_nullable = $4
                )
                "#,
            )
            .bind(&schema_name)
            .bind(column)
            .bind(data_type)
            .bind(is_nullable)
            .fetch_one(&self.pool)
            .await
            .with_context(|| {
                format!("unable to inspect required column {schema_name}.calendar_events.{column}")
            })?;
            if !present {
                invalid_calendar_lifecycle_columns
                    .push(format!("{column} {data_type} nullable={is_nullable}"));
            }
        }
        if !invalid_calendar_lifecycle_columns.is_empty() {
            bail!(
                "required column shapes {} are missing or incompatible in {schema_name}.calendar_events; LPE 0.5.0 requires an empty database initialized from crates/lpe-storage/sql/schema.sql",
                invalid_calendar_lifecycle_columns.join(", ")
            );
        }

        let deleted_object_kind_tables = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(DISTINCT table_row.relname)
            FROM pg_constraint constraint_row
            JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
            JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
            WHERE namespace_row.nspname = $1
              AND table_row.relname IN ('mail_change_log', 'mapi_object_identities')
              AND constraint_row.contype = 'c'
              AND pg_get_constraintdef(constraint_row.oid) LIKE '%deleted_calendar_event%'
            "#,
        )
        .bind(schema_name)
        .fetch_one(&self.pool)
        .await
        .with_context(|| {
            format!("unable to inspect deleted_calendar_event constraints in schema {schema_name}")
        })?;
        if deleted_object_kind_tables != 2 {
            bail!(
                "required deleted_calendar_event object-kind constraints are missing or incompatible in {schema_name}; LPE 0.5.0 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{env, str::FromStr};

    use anyhow::{Context, Result};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use uuid::Uuid;

    use super::Storage;

    const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");

    #[tokio::test]
    async fn startup_rejects_tagged_schema_without_required_mapi_identity_shape() -> Result<()> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            eprintln!("skipping schema startup guard validation; TEST_DATABASE_URL is not set");
            return Ok(());
        };

        let schema_name = format!("lpe_schema_guard_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&database_url)?)
            .await
            .context("connect to TEST_DATABASE_URL for schema startup guard validation")?;

        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
            .execute(&admin_pool)
            .await
            .context("ensure pg_trgm is available before applying schema.sql")?;
        sqlx::query(&format!("CREATE SCHEMA {schema_name}"))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("create isolated test schema {schema_name}"))?;

        let search_path = format!("{schema_name},public");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                PgConnectOptions::from_str(&database_url)?.options([("search_path", &search_path)]),
            )
            .await
            .with_context(|| format!("connect with search_path={search_path}"))?;

        let result = async {
            sqlx::raw_sql(SCHEMA_SQL)
                .execute(&pool)
                .await
                .context("apply crates/lpe-storage/sql/schema.sql")?;
            sqlx::raw_sql(
                r#"
                ALTER TABLE mapi_object_identities
                    DROP COLUMN mapi_change_number,
                    DROP COLUMN predecessor_change_list
                "#,
            )
            .execute(&pool)
            .await
            .context("remove required durable MAPI version columns")?;

            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject an incomplete tagged 0.5.0 schema");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("mapi_object_identities")
                    && message.contains("mapi_change_number")
                    && message.contains("predecessor_change_list"),
                "startup rejection must identify both missing durable MAPI version columns: {message}"
            );

            sqlx::query(
                r#"
                ALTER TABLE mapi_object_identities
                    ADD COLUMN mapi_change_number INTEGER,
                    ADD COLUMN predecessor_change_list BYTEA
                "#,
            )
            .execute(&pool)
            .await
            .context("restore incompatible MAPI version column shapes")?;
            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject incompatible durable MAPI version column shapes");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("mapi_change_number bigint NOT NULL")
                    && message.contains("predecessor_change_list bytea NOT NULL"),
                "startup rejection must identify both required durable MAPI version shapes: {message}"
            );

            sqlx::raw_sql(
                r#"
                ALTER TABLE mapi_object_identities
                    ALTER COLUMN mapi_change_number TYPE BIGINT,
                    ALTER COLUMN mapi_change_number SET NOT NULL,
                    ALTER COLUMN predecessor_change_list SET NOT NULL;
                ALTER TABLE mapi_object_identities
                    DROP CONSTRAINT mapi_object_identities_change_key_check,
                    ADD CHECK (octet_length(change_key) = 22);
                ALTER TABLE mapi_calendar_event_identity_moves
                    DROP CONSTRAINT mapi_calendar_event_identity_moves_old_change_key_check,
                    DROP CONSTRAINT mapi_calendar_event_identity_moves_new_change_key_check,
                    ADD CHECK (octet_length(old_change_key) = 22),
                    ADD CHECK (octet_length(new_change_key) = 22)
                "#,
            )
            .execute(&pool)
            .await
            .context("replace current MAPI ChangeKey XID constraints with stale 22-byte checks")?;
            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject stale MAPI ChangeKey XID constraints");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("17-24-byte MAPI ChangeKey XID constraints"),
                "startup rejection must identify stale ChangeKey XID constraints: {message}"
            );

            Ok(())
        }
        .await;

        pool.close().await;
        let cleanup = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("drop isolated test schema {schema_name}"));
        admin_pool.close().await;

        cleanup?;
        result
    }

    #[tokio::test]
    async fn startup_uses_canonical_public_schema_when_search_path_has_shadow_schema() -> Result<()>
    {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            eprintln!("skipping canonical schema startup validation; TEST_DATABASE_URL is not set");
            return Ok(());
        };

        let schema_name = format!("lpe_schema_shadow_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&database_url)?)
            .await
            .context("connect to TEST_DATABASE_URL for canonical schema validation")?;
        sqlx::query(&format!("CREATE SCHEMA {schema_name}"))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("create shadow test schema {schema_name}"))?;

        let search_path = format!("{schema_name},public");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                PgConnectOptions::from_str(&database_url)?.options([("search_path", &search_path)]),
            )
            .await
            .with_context(|| format!("connect with search_path={search_path}"))?;

        let result = Storage::new(pool.clone()).assert_schema_version().await;
        pool.close().await;
        let cleanup = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("drop shadow test schema {schema_name}"));
        admin_pool.close().await;

        cleanup?;
        result.context("startup must validate public rather than the first search_path schema")
    }

    #[tokio::test]
    async fn connect_pins_search_path_to_canonical_public_schema() -> Result<()> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            eprintln!("skipping canonical connection search_path validation; TEST_DATABASE_URL is not set");
            return Ok(());
        };

        let schema_name = format!("lpe_schema_shadow_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&database_url)?)
            .await
            .context("connect to TEST_DATABASE_URL for canonical connection validation")?;
        sqlx::raw_sql(&format!(
            "CREATE SCHEMA {schema_name}; CREATE TABLE {schema_name}.accounts (shadow_marker INTEGER)"
        ))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("create shadow accounts table in {schema_name}"))?;

        let separator = if database_url.contains('?') { '&' } else { '?' };
        let shadow_url =
            format!("{database_url}{separator}options=-c%20search_path%3D{schema_name}%2Cpublic");
        let result = async {
            let storage = Storage::connect(&shadow_url)
                .await
                .with_context(|| format!("connect with shadow schema {schema_name} first"))?;
            let active_schema = sqlx::query_scalar::<_, String>("SELECT current_schema()::text")
                .fetch_one(storage.pool())
                .await
                .context("read active schema from canonical storage connection")?;
            storage.pool.close().await;
            anyhow::ensure!(
                active_schema == "public",
                "Storage::connect left non-canonical schema {active_schema} active"
            );
            Ok(())
        }
        .await;

        let cleanup = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("drop shadow test schema {schema_name}"));
        admin_pool.close().await;

        cleanup?;
        result
    }
}
