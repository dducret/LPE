use anyhow::{bail, Context, Result};
use sqlx::{Pool, Postgres};

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
        let pool = Pool::<Postgres>::connect(database_url).await?;
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
            FROM schema_metadata
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

        self.assert_required_schema_objects().await?;

        Ok(())
    }

    async fn assert_required_schema_objects(&self) -> Result<()> {
        let schema_name = sqlx::query_scalar::<_, String>("SELECT current_schema()::text")
            .fetch_one(&self.pool)
            .await
            .context("unable to resolve the active database schema")?;

        for table in [
            "accounts",
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
    async fn startup_rejects_tagged_schema_without_mapi_identity_version_columns() -> Result<()> {
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
            sqlx::query(
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
                .assert_schema_version()
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
                .assert_schema_version()
                .await
                .expect_err("startup must reject incompatible durable MAPI version column shapes");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("mapi_change_number bigint NOT NULL")
                    && message.contains("predecessor_change_list bytea NOT NULL"),
                "startup rejection must identify both required durable MAPI version shapes: {message}"
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
}
