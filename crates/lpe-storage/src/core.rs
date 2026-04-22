use anyhow::{bail, Context, Result};
use sqlx::{Pool, Postgres};

use crate::EXPECTED_SCHEMA_VERSION;

#[derive(Clone)]
pub struct Storage {
    pub(crate) pool: Pool<Postgres>,
}

impl Storage {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = Pool::<Postgres>::connect(database_url).await?;
        let storage = Self::new(pool);
        storage.assert_schema_version().await?;
        Ok(storage)
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
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
            "database schema is not initialized for LPE 0.1.4; recreate the database and apply crates/lpe-storage/sql/schema.sql",
        )?;

        if schema_version != EXPECTED_SCHEMA_VERSION {
            bail!(
                "unsupported database schema version {schema_version}; expected {EXPECTED_SCHEMA_VERSION}. Release 0.1.5 requires a fresh database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        Ok(())
    }
}
