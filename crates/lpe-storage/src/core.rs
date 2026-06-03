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
        for table in [
            "accounts",
            "mapi_named_properties",
            "mapi_custom_property_values",
            "mapi_associated_config_messages",
            "mapi_profile_settings",
        ] {
            let present = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT to_regclass($1) IS NOT NULL
                "#,
            )
            .bind(format!("public.{table}"))
            .fetch_one(&self.pool)
            .await
            .with_context(|| format!("unable to inspect required table public.{table}"))?;

            if !present {
                bail!(
                    "required table public.{table} is missing; LPE 0.4 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
                );
            }
        }

        Ok(())
    }
}
