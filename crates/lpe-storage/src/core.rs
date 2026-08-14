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
        self.fetch_mapi_store_identity().await?;

        Ok(())
    }

    async fn assert_required_schema_objects(&self, schema_name: &str) -> Result<()> {
        for table in [
            "accounts",
            "calendar_events",
            "delegation_projection_state",
            "mapi_calendar_event_identity_moves",
            "mapi_store_identity",
            "mapi_mailbox_replicas",
            "mapi_local_replica_id_ranges",
            "mapi_local_replica_deleted_ranges",
            "mapi_object_identities",
            "mapi_special_folder_aliases",
            "mapi_named_properties",
            "mapi_custom_property_values",
            "mapi_navigation_shortcuts",
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
                    "required table {schema_name}.{table} is missing; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
                );
            }
        }

        let delegation_projection_shape_is_current = sqlx::query_scalar::<_, bool>(
            r#"
            WITH projection_table AS (
                SELECT table_row.oid
                FROM pg_class table_row
                JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
                WHERE namespace_row.nspname = $1
                  AND table_row.relname = 'delegation_projection_state'
                  AND table_row.relkind = 'r'
            ),
            accounts_table AS (
                SELECT table_row.oid
                FROM pg_class table_row
                JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
                WHERE namespace_row.nspname = $1
                  AND table_row.relname = 'accounts'
                  AND table_row.relkind = 'r'
            ),
            expected_triggers(
                trigger_name,
                table_name,
                function_name,
                trigger_type,
                update_columns
            ) AS (
                VALUES
                    (
                        'mailbox_delegation_grants_projection_change'::text,
                        'mailbox_delegation_grants'::text,
                        'track_delegation_projection_change'::text,
                        29::smallint,
                        ARRAY[]::text[]
                    ),
                    (
                        'calendar_grants_projection_change',
                        'calendar_grants',
                        'track_delegation_projection_change',
                        29,
                        ARRAY[]::text[]
                    ),
                    (
                        'sender_rights_projection_change',
                        'sender_rights',
                        'track_delegation_projection_change',
                        29,
                        ARRAY[]::text[]
                    ),
                    (
                        'delegate_preferences_projection_change',
                        'delegate_preferences',
                        'track_delegation_projection_change',
                        29,
                        ARRAY[]::text[]
                    ),
                    (
                        'mailboxes_default_delegation_projection_change',
                        'mailboxes',
                        'track_default_delegation_collection_change',
                        27,
                        ARRAY['role']::text[]
                    ),
                    (
                        'calendars_default_delegation_projection_change',
                        'calendars',
                        'track_default_delegation_collection_change',
                        27,
                        ARRAY['role']::text[]
                    ),
                    (
                        'accounts_delegate_directory_projection_change',
                        'accounts',
                        'track_delegate_directory_projection_change',
                        17,
                        ARRAY['display_name', 'primary_email']::text[]
                    )
            ),
            canonical_functions AS (
                SELECT COUNT(*) = 4 AS shape_ok
                FROM pg_proc procedure_row
                JOIN pg_namespace namespace_row
                  ON namespace_row.oid = procedure_row.pronamespace
                JOIN pg_language language_row ON language_row.oid = procedure_row.prolang
                WHERE namespace_row.nspname = $1
                  AND language_row.lanname = 'plpgsql'
                  AND procedure_row.prokind = 'f'
                  AND (
                    (
                        procedure_row.proname = 'advance_delegation_projection_state'
                        AND procedure_row.prorettype = 'void'::regtype
                        AND oidvectortypes(procedure_row.proargtypes) = 'uuid, uuid'
                        AND position('INSERT INTO delegation_projection_state' IN procedure_row.prosrc) > 0
                        AND position('WHERE EXISTS' IN procedure_row.prosrc) > 0
                        AND position('revision = delegation_projection_state.revision + 1' IN procedure_row.prosrc) > 0
                        AND position('updated_at = GREATEST' IN procedure_row.prosrc) > 0
                    )
                    OR (
                        procedure_row.proname = 'track_delegation_projection_change'
                        AND procedure_row.prorettype = 'trigger'::regtype
                        AND procedure_row.pronargs = 0
                        AND position('to_jsonb(OLD) - ARRAY[''id'', ''created_at'', ''updated_at'']' IN procedure_row.prosrc) > 0
                        AND position('mailbox_delegation_grants' IN procedure_row.prosrc) > 0
                        AND position('calendar_grants' IN procedure_row.prosrc) > 0
                        AND position('sender_rights' IN procedure_row.prosrc) > 0
                        AND position('delegate_preferences' IN procedure_row.prosrc) > 0
                        AND position('advance_delegation_projection_state(OLD.tenant_id, OLD.owner_account_id)' IN procedure_row.prosrc) > 0
                        AND position('advance_delegation_projection_state(NEW.tenant_id, NEW.owner_account_id)' IN procedure_row.prosrc) > 0
                    )
                    OR (
                        procedure_row.proname = 'track_default_delegation_collection_change'
                        AND procedure_row.prorettype = 'trigger'::regtype
                        AND procedure_row.pronargs = 0
                        AND position('TG_TABLE_NAME = ''mailboxes''' IN procedure_row.prosrc) > 0
                        AND position('OLD.role = ''inbox''' IN procedure_row.prosrc) > 0
                        AND position('TG_TABLE_NAME = ''calendars''' IN procedure_row.prosrc) > 0
                        AND position('OLD.role = ''calendar''' IN procedure_row.prosrc) > 0
                        AND position('advance_delegation_projection_state' IN procedure_row.prosrc) > 0
                    )
                    OR (
                        procedure_row.proname = 'track_delegate_directory_projection_change'
                        AND procedure_row.prorettype = 'trigger'::regtype
                        AND procedure_row.pronargs = 0
                        AND position('OLD.primary_email IS NOT DISTINCT FROM NEW.primary_email' IN procedure_row.prosrc) > 0
                        AND position('OLD.display_name IS NOT DISTINCT FROM NEW.display_name' IN procedure_row.prosrc) > 0
                        AND position('mailbox_delegation_grants' IN procedure_row.prosrc) > 0
                        AND position('calendar_grants' IN procedure_row.prosrc) > 0
                        AND position('sender_rights' IN procedure_row.prosrc) > 0
                        AND position('delegate_preferences' IN procedure_row.prosrc) > 0
                        AND position('ORDER BY owner_account_id' IN procedure_row.prosrc) > 0
                        AND position('advance_delegation_projection_state' IN procedure_row.prosrc) > 0
                    )
                  )
            )
            SELECT
                (
                    SELECT COUNT(*) = 5
                    FROM information_schema.columns
                    WHERE table_schema = $1
                      AND table_name = 'delegation_projection_state'
                      AND (
                            (column_name = 'tenant_id' AND data_type = 'uuid' AND is_nullable = 'NO')
                            OR (column_name = 'account_id' AND data_type = 'uuid' AND is_nullable = 'NO')
                            OR (column_name = 'revision' AND data_type = 'bigint' AND is_nullable = 'NO')
                            OR (column_name = 'applied_revision' AND data_type = 'bigint' AND is_nullable = 'NO')
                            OR (column_name = 'updated_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO')
                      )
                )
                AND (
                    SELECT COUNT(*) = 5
                    FROM information_schema.columns
                    WHERE table_schema = $1
                      AND table_name = 'delegation_projection_state'
                )
                AND (
                    SELECT COUNT(*) = 3
                    FROM pg_attribute attribute_row
                    JOIN pg_attrdef default_row
                      ON default_row.adrelid = attribute_row.attrelid
                     AND default_row.adnum = attribute_row.attnum
                    WHERE attribute_row.attrelid = (SELECT oid FROM projection_table)
                      AND NOT attribute_row.attisdropped
                      AND (
                            (attribute_row.attname = 'revision'
                              AND pg_get_expr(default_row.adbin, default_row.adrelid) = '1')
                            OR (attribute_row.attname = 'applied_revision'
                              AND pg_get_expr(default_row.adbin, default_row.adrelid) = '0')
                            OR (attribute_row.attname = 'updated_at'
                              AND pg_get_expr(default_row.adbin, default_row.adrelid) = 'clock_timestamp()')
                      )
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_constraint constraint_row
                    WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
                      AND constraint_row.contype = 'p'
                      AND constraint_row.convalidated
                      AND pg_get_constraintdef(constraint_row.oid) = 'PRIMARY KEY (tenant_id, account_id)'
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_constraint constraint_row
                    WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
                      AND constraint_row.contype = 'f'
                      AND constraint_row.confrelid = (SELECT oid FROM accounts_table)
                      AND constraint_row.confdeltype = 'c'
                      AND constraint_row.convalidated
                      AND (
                        SELECT array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)
                        FROM unnest(constraint_row.conkey) WITH ORDINALITY AS key_column(attnum, ordinality)
                        JOIN pg_attribute attribute_row
                          ON attribute_row.attrelid = constraint_row.conrelid
                         AND attribute_row.attnum = key_column.attnum
                      ) = ARRAY['tenant_id', 'account_id']::text[]
                      AND (
                        SELECT array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)
                        FROM unnest(constraint_row.confkey) WITH ORDINALITY AS key_column(attnum, ordinality)
                        JOIN pg_attribute attribute_row
                          ON attribute_row.attrelid = constraint_row.confrelid
                         AND attribute_row.attnum = key_column.attnum
                      ) = ARRAY['tenant_id', 'id']::text[]
                )
                AND (
                    SELECT COUNT(*) = 2
                    FROM pg_constraint constraint_row
                    WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
                      AND constraint_row.contype = 'c'
                      AND constraint_row.convalidated
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_constraint constraint_row
                    WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
                      AND constraint_row.contype = 'c'
                      AND constraint_row.convalidated
                      AND pg_get_constraintdef(constraint_row.oid) LIKE '%revision > 0%'
                      AND pg_get_constraintdef(constraint_row.oid) NOT LIKE '%applied_revision%'
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_constraint constraint_row
                    WHERE constraint_row.conrelid = (SELECT oid FROM projection_table)
                      AND constraint_row.contype = 'c'
                      AND constraint_row.convalidated
                      AND pg_get_constraintdef(constraint_row.oid) LIKE '%applied_revision >= 0%'
                      AND pg_get_constraintdef(constraint_row.oid) LIKE '%applied_revision <= revision%'
                )
                AND (SELECT shape_ok FROM canonical_functions)
                AND (
                    SELECT COUNT(*) = 7
                    FROM expected_triggers expected
                    JOIN pg_trigger trigger_row ON trigger_row.tgname = expected.trigger_name
                    JOIN pg_class table_row ON table_row.oid = trigger_row.tgrelid
                    JOIN pg_namespace table_namespace
                      ON table_namespace.oid = table_row.relnamespace
                    JOIN pg_proc procedure_row ON procedure_row.oid = trigger_row.tgfoid
                    JOIN pg_namespace procedure_namespace
                      ON procedure_namespace.oid = procedure_row.pronamespace
                    WHERE table_namespace.nspname = $1
                      AND procedure_namespace.nspname = $1
                      AND table_row.relname = expected.table_name
                      AND procedure_row.proname = expected.function_name
                      AND NOT trigger_row.tgisinternal
                      AND trigger_row.tgenabled = 'O'
                      AND trigger_row.tgtype = expected.trigger_type
                      AND trigger_row.tgnargs = 0
                      AND ARRAY(
                        SELECT attribute_row.attname::text
                        FROM unnest(trigger_row.tgattr) AS update_column(attnum)
                        JOIN pg_attribute attribute_row
                          ON attribute_row.attrelid = trigger_row.tgrelid
                         AND attribute_row.attnum = update_column.attnum
                        ORDER BY attribute_row.attname
                      ) = expected.update_columns
                )
            "#,
        )
        .bind(schema_name)
        .fetch_one(&self.pool)
        .await
        .with_context(|| {
            format!("unable to inspect delegation projection shape in {schema_name}")
        })?;
        if !delegation_projection_shape_is_current {
            bail!(
                "required delegation projection revision shape is missing or incompatible in {schema_name}; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        let mapi_custom_property_values_shape_is_current = sqlx::query_scalar::<_, bool>(
            r#"
            WITH values_table AS (
                SELECT table_row.oid
                FROM pg_class table_row
                JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
                WHERE namespace_row.nspname = $1
                  AND table_row.relname = 'mapi_custom_property_values'
                  AND table_row.relkind = 'r'
            ),
            accounts_table AS (
                SELECT table_row.oid
                FROM pg_class table_row
                JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
                WHERE namespace_row.nspname = $1
                  AND table_row.relname = 'accounts'
                  AND table_row.relkind = 'r'
            )
            SELECT EXISTS (
                SELECT 1
                FROM pg_constraint constraint_row
                WHERE constraint_row.conrelid = (SELECT oid FROM values_table)
                  AND constraint_row.contype = 'p'
                  AND constraint_row.convalidated
                  AND (
                    SELECT array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)
                    FROM unnest(constraint_row.conkey) WITH ORDINALITY AS key_column(attnum, ordinality)
                    JOIN pg_attribute attribute_row
                      ON attribute_row.attrelid = constraint_row.conrelid
                     AND attribute_row.attnum = key_column.attnum
                  ) = ARRAY[
                    'tenant_id',
                    'account_id',
                    'object_kind',
                    'canonical_id',
                    'property_tag',
                    'property_type'
                  ]::text[]
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint constraint_row
                WHERE constraint_row.conrelid = (SELECT oid FROM values_table)
                  AND constraint_row.contype = 'f'
                  AND constraint_row.confrelid = (SELECT oid FROM accounts_table)
                  AND constraint_row.confdeltype = 'c'
                  AND constraint_row.convalidated
                  AND (
                    SELECT array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)
                    FROM unnest(constraint_row.conkey) WITH ORDINALITY AS key_column(attnum, ordinality)
                    JOIN pg_attribute attribute_row
                      ON attribute_row.attrelid = constraint_row.conrelid
                     AND attribute_row.attnum = key_column.attnum
                  ) = ARRAY['tenant_id', 'account_id']::text[]
                  AND (
                    SELECT array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)
                    FROM unnest(constraint_row.confkey) WITH ORDINALITY AS key_column(attnum, ordinality)
                    JOIN pg_attribute attribute_row
                      ON attribute_row.attrelid = constraint_row.confrelid
                     AND attribute_row.attnum = key_column.attnum
                  ) = ARRAY['tenant_id', 'id']::text[]
            )
            "#,
        )
        .bind(schema_name)
        .fetch_one(&self.pool)
        .await
        .with_context(|| {
            format!("unable to inspect MAPI custom-property value shape in {schema_name}")
        })?;
        if !mapi_custom_property_values_shape_is_current {
            bail!(
                "required MAPI custom-property primary key and account cascade are missing or incompatible in {schema_name}; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
            );
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
                "required column shapes {} are missing or incompatible in {schema_name}.mapi_object_identities; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql",
                invalid_columns.join(", ")
            );
        }

        let mapi_message_move_tombstone_column_is_current = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = 'tombstones'
                  AND column_name = 'mapi_object_id'
                  AND data_type = 'bigint'
                  AND is_nullable = 'YES'
            )
            "#,
        )
        .bind(&schema_name)
        .fetch_one(&self.pool)
        .await
        .with_context(|| {
            format!(
                "unable to inspect required MAPI move tombstone column {schema_name}.tombstones.mapi_object_id"
            )
        })?;
        if !mapi_message_move_tombstone_column_is_current {
            bail!(
                "required MAPI move tombstone shape is missing or incompatible in {schema_name}; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        let mut invalid_store_identity_columns = Vec::new();
        for (column, data_type) in [
            ("singleton", "boolean"),
            ("replica_guid", "uuid"),
            ("next_global_counter", "bigint"),
        ] {
            let present = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = $1
                      AND table_name = 'mapi_store_identity'
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
                    "unable to inspect required column {schema_name}.mapi_store_identity.{column}"
                )
            })?;
            if !present {
                invalid_store_identity_columns.push(format!("{column} {data_type} NOT NULL"));
            }
        }
        if !invalid_store_identity_columns.is_empty() {
            bail!(
                "required column shapes {} are missing or incompatible in {schema_name}.mapi_store_identity; initialize a fresh database from crates/lpe-storage/sql/schema.sql",
                invalid_store_identity_columns.join(", ")
            );
        }

        let mut invalid_alias_columns = Vec::new();
        for (column, data_type) in [
            ("tenant_id", "uuid"),
            ("account_id", "uuid"),
            ("alias_folder_id", "bigint"),
            ("canonical_folder_id", "bigint"),
            ("source_key", "bytea"),
            ("mapi_change_number", "bigint"),
        ] {
            let present = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = $1
                      AND table_name = 'mapi_special_folder_aliases'
                      AND column_name = $2
                      AND data_type = $3
                      AND is_nullable = 'NO'
                )
                "#,
            )
            .bind(schema_name)
            .bind(column)
            .bind(data_type)
            .fetch_one(&self.pool)
            .await
            .with_context(|| {
                format!(
                    "unable to inspect required column {schema_name}.mapi_special_folder_aliases.{column}"
                )
            })?;
            if !present {
                invalid_alias_columns.push(format!("{column} {data_type} NOT NULL"));
            }
        }
        if !invalid_alias_columns.is_empty() {
            bail!(
                "required column shapes {} are missing or incompatible in {schema_name}.mapi_special_folder_aliases; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql",
                invalid_alias_columns.join(", ")
            );
        }

        let mapi_alias_constraints = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT constraint_row.contype::text, pg_get_constraintdef(constraint_row.oid)
            FROM pg_constraint constraint_row
            JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
            JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
            WHERE namespace_row.nspname = $1
              AND table_row.relname = 'mapi_special_folder_aliases'
            "#,
        )
        .bind(schema_name)
        .fetch_all(&self.pool)
        .await
        .with_context(|| {
            format!(
                "unable to inspect MAPI special-folder alias constraints in schema {schema_name}"
            )
        })?;
        let has_alias_constraint = |kind: &str, fragments: &[&str]| {
            mapi_alias_constraints
                .iter()
                .any(|(actual_kind, definition)| {
                    let definition = definition.replace('\'', "");
                    actual_kind == kind
                        && fragments
                            .iter()
                            .all(|fragment| definition.contains(fragment))
                })
        };

        let mapi_alias_checks_are_current =
            has_alias_constraint(
                "c",
                &[
                    "alias_folder_id >= 2818049",
                    "alias_folder_id < 9223369837831520257",
                    "65535",
                ],
            ) && has_alias_constraint(
                "c",
                &[
                    "canonical_folder_id > 0",
                    "canonical_folder_id <= 2752513",
                    "65535",
                ],
            ) && has_alias_constraint("c", &["octet_length(source_key) = 22"])
                && has_alias_constraint(
                    "c",
                    &[
                        "mapi_change_number >= 43",
                        "mapi_change_number < 140737454800896",
                    ],
                )
                && has_alias_constraint("c", &["alias_folder_id <> canonical_folder_id"]);
        if !mapi_alias_checks_are_current {
            bail!(
                "required MAPI special-folder alias CHECK constraints are missing or incompatible in {schema_name}; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        let mapi_alias_unique_constraints_are_current =
            has_alias_constraint("u", &["UNIQUE (tenant_id, account_id, source_key)"])
                && has_alias_constraint(
                    "u",
                    &["UNIQUE (tenant_id, account_id, mapi_change_number)"],
                )
                && !mapi_alias_constraints.iter().any(|(kind, definition)| {
                    kind == "u" && definition.contains("canonical_folder_id")
                });
        if !mapi_alias_unique_constraints_are_current {
            bail!(
                "required MAPI special-folder alias UNIQUE constraints are missing or incompatible in {schema_name}; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        let mapi_alias_scope_constraints_are_current = has_alias_constraint(
            "p",
            &["PRIMARY KEY (tenant_id, account_id, alias_folder_id)"],
        ) && has_alias_constraint(
            "f",
            &[
                "FOREIGN KEY (tenant_id, account_id)",
                "REFERENCES accounts(tenant_id, id)",
                "ON DELETE CASCADE",
            ],
        );
        if !mapi_alias_scope_constraints_are_current {
            bail!(
                "required MAPI special-folder alias primary key or account foreign key is missing or incompatible in {schema_name}; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        let mapi_global_identity_constraints = sqlx::query_as::<_, (String, String, String)>(
            r#"
            SELECT table_row.relname, constraint_row.contype::text,
                   pg_get_constraintdef(constraint_row.oid)
            FROM pg_constraint constraint_row
            JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
            JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
            WHERE namespace_row.nspname = $1
              AND table_row.relname IN (
                    'mapi_store_identity',
                    'mapi_object_identities',
                    'mapi_local_replica_id_ranges'
              )
            "#,
        )
        .bind(schema_name)
        .fetch_all(&self.pool)
        .await
        .with_context(|| {
            format!("unable to inspect global MAPI identity constraints in schema {schema_name}")
        })?;
        let has_global_identity_constraint = |table: &str, kind: &str, fragments: &[&str]| {
            mapi_global_identity_constraints.iter().any(
                |(actual_table, actual_kind, definition)| {
                    actual_table == table
                        && actual_kind == kind
                        && fragments
                            .iter()
                            .all(|fragment| definition.contains(fragment))
                },
            )
        };
        let global_identity_constraints_are_current = has_global_identity_constraint(
            "mapi_store_identity",
            "p",
            &["PRIMARY KEY (singleton)"],
        ) && has_global_identity_constraint(
            "mapi_store_identity",
            "c",
            &["singleton = true"],
        ) && [
            "mapi_global_counter",
            "mapi_object_id",
            "source_key",
            "mapi_change_number",
        ]
        .into_iter()
        .all(|column| {
            has_global_identity_constraint(
                "mapi_object_identities",
                "u",
                &[&format!("UNIQUE ({column})")],
            )
        }) && has_global_identity_constraint(
            "mapi_local_replica_id_ranges",
            "x",
            &[
                "EXCLUDE USING gist",
                "int8range(first_global_counter, end_global_counter_exclusive",
                "WITH &&",
            ],
        ) && !mapi_global_identity_constraints
            .iter()
            .any(|(table, kind, definition)| {
                table == "mapi_local_replica_id_ranges"
                    && kind == "x"
                    && (definition.contains("tenant_id WITH =")
                        || definition.contains("account_id WITH ="))
            });
        if !global_identity_constraints_are_current {
            bail!(
                "required global MAPI identity constraints are missing or incompatible in {schema_name}; initialize a fresh database from crates/lpe-storage/sql/schema.sql"
            );
        }

        let mapi_outlook_cache_fidelity_shape_is_current = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT
                (
                    SELECT COUNT(*) = 6
                    FROM information_schema.columns
                    WHERE table_schema = $1
                      AND table_name = 'mapi_navigation_shortcuts'
                      AND (
                            (column_name = 'ordinal' AND data_type = 'bytea' AND is_nullable = 'NO' AND column_default IS NULL)
                            OR (column_name = 'calendar_color' AND data_type = 'integer' AND is_nullable = 'YES' AND column_default IS NULL)
                            OR (column_name = 'address_book_entry_id' AND data_type = 'bytea' AND is_nullable = 'YES' AND column_default IS NULL)
                            OR (column_name = 'address_book_store_entry_id' AND data_type = 'bytea' AND is_nullable = 'YES' AND column_default IS NULL)
                            OR (column_name = 'client_id' AND data_type = 'bytea' AND is_nullable = 'YES' AND column_default IS NULL)
                            OR (column_name = 'ro_group_type' AND data_type = 'integer' AND is_nullable = 'YES' AND column_default IS NULL)
                      )
                )
                AND (
                    SELECT COUNT(*) = 6
                    FROM pg_constraint constraint_row
                    JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
                    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
                    WHERE namespace_row.nspname = $1
                      AND table_row.relname = 'mapi_navigation_shortcuts'
                      AND constraint_row.contype = 'c'
                      AND (
                            (constraint_row.conname = 'mapi_navigation_shortcuts_ordinal_check'
                             AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(ordinal) > 0%'
                             AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(ordinal) <= 65535%'
                             AND pg_get_constraintdef(constraint_row.oid) LIKE '%get_byte(ordinal, (octet_length(ordinal) - 1)) <> 0%'
                             AND pg_get_constraintdef(constraint_row.oid) LIKE '%get_byte(ordinal, (octet_length(ordinal) - 1)) <> 255%')
                            OR (constraint_row.conname = 'mapi_navigation_shortcuts_calendar_color_check'
                                AND replace(pg_get_constraintdef(constraint_row.oid), '''', '') LIKE '%calendar_color >= -1%'
                                AND pg_get_constraintdef(constraint_row.oid) LIKE '%calendar_color <= 14%')
                            OR (constraint_row.conname = 'mapi_navigation_shortcuts_address_book_entry_id_check'
                                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(address_book_entry_id) > 0%'
                                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(address_book_entry_id) <= 65535%')
                            OR (constraint_row.conname = 'mapi_navigation_shortcuts_address_book_store_entry_id_check'
                                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(address_book_store_entry_id) > 0%'
                                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(address_book_store_entry_id) <= 65535%')
                            OR (constraint_row.conname = 'mapi_navigation_shortcuts_client_id_check'
                                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(client_id) > 0%'
                                AND pg_get_constraintdef(constraint_row.oid) LIKE '%octet_length(client_id) <= 65535%')
                            OR (constraint_row.conname = 'mapi_navigation_shortcuts_ro_group_type_check'
                                AND replace(pg_get_constraintdef(constraint_row.oid), '''', '') LIKE '%ro_group_type >= -1%'
                                AND pg_get_constraintdef(constraint_row.oid) LIKE '%ro_group_type <= 4%')
                      )
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_index index_row
                    JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
                    JOIN pg_class table_row ON table_row.oid = index_row.indrelid
                    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
                    WHERE namespace_row.nspname = $1
                      AND table_row.relname = 'mapi_navigation_shortcuts'
                      AND index_class.relname = 'mapi_navigation_shortcuts_account_idx'
                      AND NOT index_row.indisunique
                      AND pg_get_indexdef(index_row.indexrelid) LIKE '%USING btree (tenant_id, account_id, section, ordinal, subject, id)'
                )
                AND EXISTS (
                    SELECT 1
                    FROM pg_index index_row
                    JOIN pg_class index_class ON index_class.oid = index_row.indexrelid
                    JOIN pg_class table_row ON table_row.oid = index_row.indrelid
                    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
                    WHERE namespace_row.nspname = $1
                      AND table_row.relname = 'mapi_associated_config_messages'
                      AND index_class.relname = 'mapi_associated_config_messages_logical_idx'
                      AND NOT index_row.indisunique
                      AND pg_get_indexdef(index_row.indexrelid) LIKE '%USING btree (tenant_id, account_id, folder_id, message_class, subject)'
                )
            "#,
        )
        .bind(schema_name)
        .fetch_one(&self.pool)
        .await
        .with_context(|| {
            format!(
                "unable to inspect MAPI WLink/configuration FAI fidelity shape in schema {schema_name}"
            )
        })?;
        if !mapi_outlook_cache_fidelity_shape_is_current {
            bail!(
                "required MAPI WLink/configuration FAI fidelity shape is missing or incompatible in {schema_name}; initialize an empty LPE 0.5.2 database from crates/lpe-storage/sql/schema.sql"
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
                "required 17-24-byte MAPI ChangeKey XID constraints are missing or incompatible in {schema_name}; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
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
                "required column shapes {} are missing or incompatible in {schema_name}.calendar_events; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql",
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
                "required deleted_calendar_event object-kind constraints are missing or incompatible in {schema_name}; LPE 0.5.2 requires an empty database initialized from crates/lpe-storage/sql/schema.sql"
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
    async fn startup_rejects_tagged_schema_without_required_mapi_shape() -> Result<()> {
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

            for table in [
                "mapi_local_replica_id_ranges",
                "mapi_local_replica_deleted_ranges",
            ] {
                let hidden_table = format!("{table}_missing");
                sqlx::query(&format!("ALTER TABLE {table} RENAME TO {hidden_table}"))
                    .execute(&pool)
                    .await
                    .with_context(|| format!("temporarily hide required table {table}"))?;
                let error = Storage::new(pool.clone())
                    .assert_required_schema_objects(&schema_name)
                    .await
                    .expect_err("startup must reject a missing local replica range table");
                let message = format!("{error:#}");
                anyhow::ensure!(
                    message.contains(table),
                    "startup rejection must identify missing table {table}: {message}"
                );
                sqlx::query(&format!("ALTER TABLE {hidden_table} RENAME TO {table}"))
                    .execute(&pool)
                    .await
                    .with_context(|| format!("restore required table {table}"))?;
            }

            sqlx::query(
                "ALTER TABLE mapi_navigation_shortcuts ALTER COLUMN ordinal DROP NOT NULL",
            )
            .execute(&pool)
            .await
            .context("make the WLink ordinal nullable")?;
            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject an incompatible WLink ordinal shape");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("MAPI WLink/configuration FAI fidelity shape"),
                "startup rejection must identify the invalid WLink ordinal shape: {message}"
            );
            sqlx::query(
                "ALTER TABLE mapi_navigation_shortcuts ALTER COLUMN ordinal SET NOT NULL",
            )
            .execute(&pool)
            .await
            .context("restore the WLink ordinal nullability")?;

            sqlx::raw_sql(
                r#"
                DROP INDEX mapi_associated_config_messages_logical_idx;
                CREATE UNIQUE INDEX mapi_associated_config_messages_logical_idx
                    ON mapi_associated_config_messages (
                        tenant_id, account_id, folder_id, message_class, subject
                    );
                "#,
            )
            .execute(&pool)
            .await
            .context("replace the FAI logical lookup index with the stale unique index")?;
            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject the stale unique FAI logical index");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("MAPI WLink/configuration FAI fidelity shape"),
                "startup rejection must identify the stale unique FAI logical index: {message}"
            );
            sqlx::raw_sql(
                r#"
                DROP INDEX mapi_associated_config_messages_logical_idx;
                CREATE INDEX mapi_associated_config_messages_logical_idx
                    ON mapi_associated_config_messages (
                        tenant_id, account_id, folder_id, message_class, subject
                    );
                "#,
            )
            .execute(&pool)
            .await
            .context("restore the non-unique FAI logical lookup index")?;

            sqlx::query(
                r#"
                ALTER TABLE mapi_special_folder_aliases
                    ALTER COLUMN mapi_change_number DROP NOT NULL
                "#,
            )
            .execute(&pool)
            .await
            .context("make the special-folder alias CN nullable")?;
            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject an incompatible special-folder alias CN");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("mapi_special_folder_aliases")
                    && message.contains("mapi_change_number bigint NOT NULL"),
                "startup rejection must identify the invalid special-folder alias CN: {message}"
            );
            sqlx::query(
                r#"
                ALTER TABLE mapi_special_folder_aliases
                    ALTER COLUMN mapi_change_number SET NOT NULL
                "#,
            )
            .execute(&pool)
            .await
            .context("restore the special-folder alias CN nullability")?;

            sqlx::query(
                r#"
                ALTER TABLE mapi_special_folder_aliases
                    DROP CONSTRAINT mapi_special_folder_aliases_alias_folder_id_check
                "#,
            )
            .execute(&pool)
            .await
            .context("remove the special-folder alias FID range check")?;
            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject a missing special-folder alias CHECK");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("MAPI special-folder alias CHECK constraints"),
                "startup rejection must identify the missing special-folder alias CHECK: {message}"
            );
            sqlx::query(
                r#"
                ALTER TABLE mapi_special_folder_aliases
                    ADD CHECK (
                        alias_folder_id >= 2818049
                        AND alias_folder_id < 9223369837831520257
                        AND (alias_folder_id & 65535) = 1
                    )
                "#,
            )
            .execute(&pool)
            .await
            .context("restore the special-folder alias FID range check")?;

            sqlx::raw_sql(
                r#"
                DO $$
                DECLARE
                    constraint_name TEXT;
                BEGIN
                    FOR constraint_name IN
                        SELECT conname
                        FROM pg_constraint
                        WHERE conrelid = 'mapi_special_folder_aliases'::regclass
                          AND contype = 'u'
                    LOOP
                        EXECUTE format(
                            'ALTER TABLE mapi_special_folder_aliases DROP CONSTRAINT %I',
                            constraint_name
                        );
                    END LOOP;
                END;
                $$;
                "#,
            )
            .execute(&pool)
            .await
            .context("remove the special-folder alias uniqueness constraints")?;
            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject missing special-folder alias UNIQUE constraints");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("MAPI special-folder alias UNIQUE constraints"),
                "startup rejection must identify missing special-folder alias UNIQUE constraints: {message}"
            );
            sqlx::raw_sql(
                r#"
                ALTER TABLE mapi_special_folder_aliases
                    ADD UNIQUE (tenant_id, account_id, source_key),
                    ADD UNIQUE (tenant_id, account_id, mapi_change_number)
                "#,
            )
            .execute(&pool)
            .await
            .context("restore the special-folder alias uniqueness constraints")?;

            sqlx::query("ALTER TABLE tombstones DROP COLUMN mapi_object_id")
                .execute(&pool)
                .await
                .context("remove the retained MAPI move tombstone ID")?;
            let error = Storage::new(pool.clone())
                .assert_required_schema_objects(&schema_name)
                .await
                .expect_err("startup must reject a missing MAPI move tombstone ID");
            let message = format!("{error:#}");
            anyhow::ensure!(
                message.contains("MAPI move tombstone shape"),
                "startup rejection must identify the missing MAPI move tombstone ID: {message}"
            );
            sqlx::raw_sql(
                r#"
                ALTER TABLE tombstones
                    ADD COLUMN mapi_object_id BIGINT CHECK (
                        mapi_object_id IS NULL
                        OR (
                            object_kind = 'mailbox_message'
                            AND mapi_object_id > 0
                            AND (mapi_object_id & 65535) = 1
                        )
                    )
                "#,
            )
            .execute(&pool)
            .await
            .context("restore the retained MAPI move tombstone ID")?;

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
                .expect_err("startup must reject an incomplete tagged 0.5.2 schema");
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
                    ALTER COLUMN predecessor_change_list SET NOT NULL,
                    ADD UNIQUE (mapi_change_number);
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
