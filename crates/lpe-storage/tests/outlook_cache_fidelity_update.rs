use std::{env, str::FromStr};

use anyhow::{Context, Result};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Row,
};
use uuid::Uuid;

const UPDATE_SQL: &str = include_str!("../sql/updates/0.5.0-sql-v1-outlook-cache-fidelity.sql");

#[tokio::test]
async fn outlook_cache_fidelity_update_runs_twice_and_rolls_back_rejected_shapes() -> Result<()> {
    let Some(database_url) = env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("skipping Outlook cache fidelity update test; TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let schema_name = format!("lpe_outlook_update_{}", Uuid::new_v4().simple());
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&database_url)?)
        .await
        .context("connect to TEST_DATABASE_URL for Outlook cache fidelity update")?;

    let result = run_update_scenarios(&pool, &schema_name).await;
    let cleanup = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        .execute(&pool)
        .await
        .with_context(|| format!("drop isolated update test schema {schema_name}"));
    pool.close().await;

    cleanup?;
    result
}

async fn run_update_scenarios(pool: &PgPool, schema_name: &str) -> Result<()> {
    recreate_legacy_schema(pool, schema_name, "0.5.0-sql-v1", false).await?;
    let update_sql = update_sql_for_schema(schema_name)?;

    execute_update(pool, &update_sql)
        .await
        .context("apply Outlook cache fidelity update to the legacy 0.5.0 shape")?;
    assert_successful_update(pool, schema_name).await?;

    execute_update(pool, &update_sql)
        .await
        .context("apply Outlook cache fidelity update a second time")?;
    assert_successful_update(pool, schema_name).await?;

    recreate_legacy_schema(pool, schema_name, "0.4.9-sql-v1", false).await?;
    let error = execute_update_expect_failure(pool, &update_sql).await?;
    anyhow::ensure!(
        error.contains("unsupported LPE schema version"),
        "pre-0.5 rejection must identify the unsupported version: {error}"
    );
    assert_legacy_shape_unchanged(pool, schema_name, false).await?;

    recreate_legacy_schema(pool, schema_name, "0.5.0-sql-v1", true).await?;
    let error = execute_update_expect_failure(pool, &update_sql).await?;
    anyhow::ensure!(
        error.contains("MAPI local replica range table shape is incomplete"),
        "incomplete 0.5.0 rejection must identify the invalid range shape: {error}"
    );
    assert_legacy_shape_unchanged(pool, schema_name, true).await?;

    Ok(())
}

fn update_sql_for_schema(schema_name: &str) -> Result<String> {
    let rewritten = UPDATE_SQL
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

async fn recreate_legacy_schema(
    pool: &PgPool,
    schema_name: &str,
    schema_version: &str,
    create_incomplete_range_table: bool,
) -> Result<()> {
    let incomplete_range_table = create_incomplete_range_table.then(|| {
        format!(
            r#"
            CREATE TABLE {schema_name}.mapi_local_replica_id_ranges (
                tenant_id UUID NOT NULL,
                account_id UUID NOT NULL,
                replica_guid UUID NOT NULL,
                first_global_counter BIGINT NOT NULL,
                end_global_counter_exclusive BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#
        )
    });
    let ddl = format!(
        r#"
        DROP SCHEMA IF EXISTS {schema_name} CASCADE;
        CREATE SCHEMA {schema_name};

        CREATE TABLE {schema_name}.schema_metadata (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton = TRUE),
            schema_version TEXT NOT NULL CHECK (schema_version = '{schema_version}'),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        INSERT INTO {schema_name}.schema_metadata (singleton, schema_version)
        VALUES (TRUE, '{schema_version}');

        CREATE TABLE {schema_name}.accounts (
            tenant_id UUID NOT NULL,
            id UUID NOT NULL,
            PRIMARY KEY (tenant_id, id)
        );
        INSERT INTO {schema_name}.accounts (tenant_id, id)
        VALUES (
            '10000000-0000-0000-0000-000000000001',
            '20000000-0000-0000-0000-000000000001'
        );

        CREATE TABLE {schema_name}.mapi_mailbox_replicas (
            tenant_id UUID NOT NULL,
            account_id UUID NOT NULL,
            replica_guid UUID NOT NULL,
            next_global_counter BIGINT NOT NULL DEFAULT 43 CHECK (next_global_counter >= 43),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (tenant_id, account_id),
            UNIQUE (tenant_id, account_id, replica_guid),
            FOREIGN KEY (tenant_id, account_id)
                REFERENCES {schema_name}.accounts (tenant_id, id) ON DELETE CASCADE
        );

        CREATE TABLE {schema_name}.mapi_navigation_shortcuts (
            tenant_id UUID NOT NULL,
            id UUID NOT NULL,
            account_id UUID NOT NULL,
            subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
            target_folder_id BIGINT CHECK (target_folder_id IS NULL OR target_folder_id > 0),
            shortcut_type BIGINT NOT NULL CHECK (shortcut_type >= 0 AND shortcut_type <= 4294967295),
            flags BIGINT NOT NULL DEFAULT 0 CHECK (flags >= 0 AND flags <= 4294967295),
            save_stamp BIGINT NOT NULL DEFAULT 0 CHECK (save_stamp >= 0 AND save_stamp <= 4294967295),
            section BIGINT NOT NULL DEFAULT 0 CHECK (section >= 0 AND section <= 4294967295),
            ordinal BIGINT NOT NULL DEFAULT 0 CHECK (ordinal >= 0 AND ordinal <= 4294967295),
            group_header_id UUID,
            group_name TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (tenant_id, id),
            FOREIGN KEY (tenant_id, account_id)
                REFERENCES {schema_name}.accounts (tenant_id, id) ON DELETE CASCADE
        );
        CREATE INDEX mapi_navigation_shortcuts_account_idx
            ON {schema_name}.mapi_navigation_shortcuts
                (tenant_id, account_id, section, ordinal, subject, id);

        INSERT INTO {schema_name}.mapi_navigation_shortcuts (
            tenant_id, id, account_id, subject, shortcut_type, ordinal
        ) VALUES
            ('10000000-0000-0000-0000-000000000001', '30000000-0000-0000-0000-000000000001', '20000000-0000-0000-0000-000000000001', 'ordinal-0', 0, 0),
            ('10000000-0000-0000-0000-000000000001', '30000000-0000-0000-0000-000000000002', '20000000-0000-0000-0000-000000000001', 'ordinal-1', 0, 1),
            ('10000000-0000-0000-0000-000000000001', '30000000-0000-0000-0000-000000000003', '20000000-0000-0000-0000-000000000001', 'ordinal-254', 0, 254),
            ('10000000-0000-0000-0000-000000000001', '30000000-0000-0000-0000-000000000004', '20000000-0000-0000-0000-000000000001', 'ordinal-255', 0, 255),
            ('10000000-0000-0000-0000-000000000001', '30000000-0000-0000-0000-000000000005', '20000000-0000-0000-0000-000000000001', 'ordinal-256', 0, 256),
            ('10000000-0000-0000-0000-000000000001', '30000000-0000-0000-0000-000000000006', '20000000-0000-0000-0000-000000000001', 'ordinal-257', 0, 257),
            ('10000000-0000-0000-0000-000000000001', '30000000-0000-0000-0000-000000000007', '20000000-0000-0000-0000-000000000001', 'ordinal-4294967295', 0, 4294967295);

        CREATE TABLE {schema_name}.mapi_associated_config_messages (
            tenant_id UUID NOT NULL,
            id UUID NOT NULL,
            account_id UUID NOT NULL,
            folder_id BIGINT NOT NULL CHECK (folder_id > 0),
            message_class TEXT NOT NULL CHECK (btrim(message_class) <> ''),
            subject TEXT NOT NULL CHECK (btrim(subject) <> ''),
            properties_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (tenant_id, id),
            FOREIGN KEY (tenant_id, account_id)
                REFERENCES {schema_name}.accounts (tenant_id, id) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX mapi_associated_config_messages_logical_idx
            ON {schema_name}.mapi_associated_config_messages
                (tenant_id, account_id, folder_id, message_class, subject);

        {}
        "#,
        incomplete_range_table.unwrap_or_default()
    );

    sqlx::raw_sql(&ddl)
        .execute(pool)
        .await
        .with_context(|| format!("create legacy migration fixture in {schema_name}"))?;
    Ok(())
}

async fn execute_update(pool: &PgPool, update_sql: &str) -> Result<()> {
    let mut connection = pool
        .acquire()
        .await
        .context("acquire migration connection")?;
    match sqlx::raw_sql(update_sql).execute(&mut *connection).await {
        Ok(_) => Ok(()),
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error.into())
        }
    }
}

async fn execute_update_expect_failure(pool: &PgPool, update_sql: &str) -> Result<String> {
    let mut connection = pool
        .acquire()
        .await
        .context("acquire migration connection")?;
    let error = sqlx::raw_sql(update_sql)
        .execute(&mut *connection)
        .await
        .expect_err("the migration fixture must be rejected");
    sqlx::query("ROLLBACK")
        .execute(&mut *connection)
        .await
        .context("roll back the expected migration failure")?;
    Ok(error.to_string())
}

async fn assert_successful_update(pool: &PgPool, schema_name: &str) -> Result<()> {
    let rows = sqlx::query(&format!(
        "SELECT subject, ordinal FROM {schema_name}.mapi_navigation_shortcuts ORDER BY subject"
    ))
    .fetch_all(pool)
    .await
    .context("read converted WLink ordinals")?;
    let actual = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("subject"),
                row.get::<Vec<u8>, _>("ordinal"),
            )
        })
        .collect::<Vec<_>>();
    let expected = vec![
        ("ordinal-0".to_owned(), vec![0x00, 0x00, 0x00, 0x00, 0x80]),
        ("ordinal-1".to_owned(), vec![0x01]),
        ("ordinal-254".to_owned(), vec![0xFE]),
        ("ordinal-255".to_owned(), vec![0x00, 0x00, 0x00, 0xFF, 0x80]),
        ("ordinal-256".to_owned(), vec![0x00, 0x00, 0x01, 0x00, 0x80]),
        ("ordinal-257".to_owned(), vec![0x01, 0x01]),
        (
            "ordinal-4294967295".to_owned(),
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0x80],
        ),
    ];
    anyhow::ensure!(
        actual == expected,
        "unexpected ordinal conversion: {actual:?}"
    );
    let mut distinct_ordinals = actual
        .iter()
        .map(|(_, ordinal)| ordinal.clone())
        .collect::<Vec<_>>();
    distinct_ordinals.sort();
    distinct_ordinals.dedup();
    anyhow::ensure!(
        distinct_ordinals.len() == actual.len(),
        "legacy WLink ordinal conversion must be injective"
    );
    anyhow::ensure!(
        actual.iter().all(|(_, ordinal)| {
            ordinal
                .last()
                .is_some_and(|last| *last != 0x00 && *last != 0xFF)
        }),
        "every converted WLink ordinal must end in neither 0x00 nor 0xFF"
    );

    let optional_column_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'mapi_navigation_shortcuts'
          AND column_name IN (
              'calendar_color', 'address_book_entry_id',
              'address_book_store_entry_id', 'client_id', 'ro_group_type'
          )
        "#,
    )
    .bind(schema_name)
    .fetch_one(pool)
    .await
    .context("count migrated WLink client-property columns")?;
    anyhow::ensure!(
        optional_column_count == 5,
        "the update must add all five WLink client-property columns"
    );
    anyhow::ensure!(
        !logical_index_is_unique(pool, schema_name).await?,
        "the associated-configuration logical index must become non-unique"
    );

    for table in [
        "mapi_local_replica_id_ranges",
        "mapi_local_replica_deleted_ranges",
    ] {
        anyhow::ensure!(
            relation_exists(pool, schema_name, table).await?,
            "the update must create {table}"
        );
    }
    Ok(())
}

async fn assert_legacy_shape_unchanged(
    pool: &PgPool,
    schema_name: &str,
    incomplete_range_table_existed: bool,
) -> Result<()> {
    let ordinal_type = sqlx::query_scalar::<_, String>(
        r#"
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'mapi_navigation_shortcuts'
          AND column_name = 'ordinal'
        "#,
    )
    .bind(schema_name)
    .fetch_one(pool)
    .await
    .context("read legacy WLink ordinal type after rejected update")?;
    anyhow::ensure!(
        ordinal_type == "bigint",
        "a rejected update must roll the WLink ordinal back to bigint"
    );

    let optional_column_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'mapi_navigation_shortcuts'
          AND column_name IN (
              'calendar_color', 'address_book_entry_id',
              'address_book_store_entry_id', 'client_id', 'ro_group_type'
          )
        "#,
    )
    .bind(schema_name)
    .fetch_one(pool)
    .await
    .context("count WLink columns after rejected update")?;
    anyhow::ensure!(
        optional_column_count == 0,
        "a rejected update must not leave additive WLink columns"
    );
    anyhow::ensure!(
        logical_index_is_unique(pool, schema_name).await?,
        "a rejected update must preserve the legacy unique FAI index"
    );
    anyhow::ensure!(
        relation_exists(pool, schema_name, "mapi_local_replica_id_ranges").await?
            == incomplete_range_table_existed,
        "a rejected update changed the preexisting local range table state"
    );
    anyhow::ensure!(
        !relation_exists(pool, schema_name, "mapi_local_replica_deleted_ranges").await?,
        "a rejected update must roll back the created deleted-range table"
    );
    Ok(())
}

async fn logical_index_is_unique(pool: &PgPool, schema_name: &str) -> Result<bool> {
    sqlx::query_scalar(
        r#"
        SELECT index_row.indisunique
        FROM pg_index index_row
        JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
        JOIN pg_namespace namespace_row ON namespace_row.oid = index_class.relnamespace
        WHERE namespace_row.nspname = $1
          AND index_class.relname = 'mapi_associated_config_messages_logical_idx'
        "#,
    )
    .bind(schema_name)
    .fetch_one(pool)
    .await
    .context("read associated-configuration logical index uniqueness")
}

async fn relation_exists(pool: &PgPool, schema_name: &str, table_name: &str) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_class table_row
            JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
            WHERE namespace_row.nspname = $1
              AND table_row.relname = $2
              AND table_row.relkind = 'r'
        )
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_one(pool)
    .await
    .with_context(|| format!("inspect relation {schema_name}.{table_name}"))
}
