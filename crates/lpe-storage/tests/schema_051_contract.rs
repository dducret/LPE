const SCHEMA: &str = include_str!("../sql/schema.sql");
const PREFLIGHT: &str = include_str!("../sql/updates/0.5.0-sql-v1-to-0.5.1-sql-preflight.sql");
const TRANSITION: &str = include_str!("../sql/updates/0.5.0-sql-v1-to-0.5.1-sql.sql");
const UPDATE_LPE: &str = include_str!("../../../installation/debian-trixie/update-lpe.sh");
const CHECK_LPE: &str = include_str!("../../../installation/debian-trixie/check-lpe.sh");
const INIT_LPE: &str = include_str!("../../../installation/debian-trixie/init-schema.sh");
const INSTALL_COMMON: &str =
    include_str!("../../../installation/debian-trixie/lib/install-common.sh");
const CALENDAR_MAIL_CLASSIFICATION_SHAPE: &str =
    include_str!("../../../installation/debian-trixie/lib/calendar-mail-classification-shape.sql");
const ROOT_README: &str = include_str!("../../../README.md");
const INSTALLATION_README: &str = include_str!("../../../installation/README.md");
const AGENT_INSTRUCTIONS: &str = include_str!("../../../AGENTS.md");
const SQL_SCHEMA_ARCHITECTURE: &str = include_str!("../../../docs/architecture/sql-schema-v2.md");
const RELEASE_053: &str = include_str!("../../../docs/releases/0.5.3.md");

#[test]
fn canonical_schema_and_release_contract_use_052_schema_label() {
    assert!(
        SCHEMA.contains("schema_version = '0.5.2-sql'")
            && SCHEMA.contains("VALUES (TRUE, '0.5.2-sql')"),
        "the canonical schema must use the exact 0.5.2-sql release label"
    );
    for (name, contract) in [
        ("README.md", ROOT_README),
        ("installation/README.md", INSTALLATION_README),
        ("AGENTS.md", AGENT_INSTRUCTIONS),
        ("sql-schema-v2.md", SQL_SCHEMA_ARCHITECTURE),
        ("0.5.3 release notes", RELEASE_053),
    ] {
        assert!(
            contract.contains("0.5.2-sql") && !contract.contains("0.5.3-sql"),
            "{name} must agree with the canonical 0.5.2-sql deployment contract"
        );
    }
}

#[test]
fn update_script_rejects_noncanonical_schema_before_service_stop_or_mutation() {
    assert_contains_all(
        "update-lpe.sh",
        UPDATE_LPE,
        &[
            "SELECT schema_version FROM public.schema_metadata WHERE singleton = TRUE",
            "EXPECTED_SCHEMA_VERSION",
            "EXPECTED_RELEASE_VERSION",
            "if [[ \"${INSTALLED_SCHEMA_VERSION}\" != \"${EXPECTED_SCHEMA_VERSION}\" ]]",
            "has no in-place schema upgrade path",
            "source \"${SCRIPT_DIR}/lib/install-common.sh\"",
            "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${EXPECTED_SCHEMA_VERSION}\"",
            "Database schema ${EXPECTED_SCHEMA_VERSION} is current",
        ],
    );
    assert_contains_all(
        "install-common.sh canonical schema guard",
        INSTALL_COMMON,
        &[
            "canonical_schema_shape_is_current()",
            "local expected_schema_version=\"$2\"",
            "schema_metadata_shape_ok \"${database_url}\" \"${expected_schema_version}\"",
            "calendar_meeting_response_state_shape_ok \"${database_url}\"",
            "calendar_meeting_request_correlation_index_shape_ok \"${database_url}\"",
            "calendar_events_meeting_response_state_json_object_check",
            "calendar_events_active_uid_correlation_idx",
            "pg_get_expr(default_row.adbin, default_row.adrelid) = '''{}''::jsonb'",
            "constraint_row.convalidated",
            "pg_get_expr(constraint_row.conbin, constraint_row.conrelid) =",
            "mapi_store_identity_shape_ok \"${database_url}\"",
        ],
    );
    for forbidden in [
        "SOURCE_SCHEMA_VERSION",
        "SCHEMA_051_PREFLIGHT_FILE",
        "OUTLOOK_CACHE_FIDELITY_UPDATE_FILE",
        "SCHEMA_051_UPDATE_FILE",
        "MIGRATE_SCHEMA_FROM_050",
        "schema_target_shape_validated",
        "0.5.0-sql-v1-to-0.5.1-sql",
        "0.5.0-sql-v1-outlook-cache-fidelity.sql",
        "psql \"${DATABASE_URL}\" -X -v ON_ERROR_STOP=1 -f",
    ] {
        assert!(
            !UPDATE_LPE.contains(forbidden),
            "update-lpe.sh must not retain a 0.5.1 migration path: {forbidden}"
        );
    }
    assert_before(
        UPDATE_LPE,
        "source \"${SCRIPT_DIR}/lib/install-common.sh\"",
        "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${EXPECTED_SCHEMA_VERSION}\"",
        "the updater must source the canonical schema guard before invoking it",
    );
    assert_before(
        UPDATE_LPE,
        "if [[ \"${INSTALLED_SCHEMA_VERSION}\" != \"${EXPECTED_SCHEMA_VERSION}\" ]]",
        "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${EXPECTED_SCHEMA_VERSION}\"",
        "the updater must reject unsupported labels before it checks the current schema shape",
    );
    assert_before(
        UPDATE_LPE,
        "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${EXPECTED_SCHEMA_VERSION}\"",
        "systemctl stop \"${SERVICE_NAME}\"",
        "the updater must reject incomplete current schemas before it stops LPE",
    );
    assert_before(
        UPDATE_LPE,
        "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${EXPECTED_SCHEMA_VERSION}\"",
        "write_env_value \"${ENV_FILE}\"",
        "the updater must reject incomplete current schemas before it mutates deployment state",
    );
    assert_before(
        UPDATE_LPE,
        "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${EXPECTED_SCHEMA_VERSION}\"",
        "\"${CARGO_BIN}\" build --release -p lpe-cli",
        "the updater must reject incomplete current schemas before it builds LPE",
    );
    assert_before(
        UPDATE_LPE,
        "if [[ \"${INSTALLED_SCHEMA_VERSION}\" != \"${EXPECTED_SCHEMA_VERSION}\" ]]",
        "write_env_value \"${ENV_FILE}\"",
        "the updater must reject unsupported labels before it mutates deployment state",
    );
    assert_before(
        UPDATE_LPE,
        "if [[ \"${INSTALLED_SCHEMA_VERSION}\" != \"${EXPECTED_SCHEMA_VERSION}\" ]]",
        "\"${CARGO_BIN}\" build --release -p lpe-cli",
        "the updater must reject unsupported labels before it builds LPE",
    );
}

#[test]
fn installer_validates_calendar_request_correlation_index_shape() {
    for (name, script) in [("init-schema.sh", INIT_LPE), ("check-lpe.sh", CHECK_LPE)] {
        assert_contains_all(
            name,
            script,
            &[
                "calendar_meeting_request_correlation_index_shape_ok",
                "calendar_meeting_request_correlation_index_shape_status",
            ],
        );
    }
    assert_contains_all(
        "install-common.sh meeting-request correlation index guard",
        INSTALL_COMMON,
        &[
            "calendar_events_active_uid_correlation_idx",
            "index_row.indnatts = 4",
            "ARRAY['tenant_id', 'owner_account_id', 'uid', 'id']::text[]",
            "(lifecycle_state = ''active''::text)",
        ],
    );
}

#[test]
fn canonical_schema_guard_requires_recovery_and_search_membership_semantics() {
    assert_contains_all(
        "shared recoverable-item shape guard",
        INSTALL_COMMON,
        &[
            "recoverable_items_shape_ok()",
            "table_name = 'recoverable_items'",
            "column_name = 'created_by_protocol' AND data_type = 'text' AND is_nullable = 'NO'",
            "UNIQUE (tenant_id, account_id, source_mailbox_message_id)",
            "pg_get_constraintdef(oid) LIKE '%''imap''%'",
            "mail_search_membership_shape_ok()",
            "constraint_row.conrelid = 'public.mail_search_documents'::regclass",
            "constraint_row.confrelid = 'public.mailbox_messages'::regclass",
            "constraint_row.confdeltype = 'c'",
            "ARRAY['tenant_id', 'account_id', 'mailbox_message_id', 'message_id']::text[]",
            "ARRAY['tenant_id', 'account_id', 'id', 'message_id']::text[]",
            "recoverable_items_shape_ok \"${database_url}\"",
            "mail_search_membership_shape_ok \"${database_url}\"",
        ],
    );
    assert!(
        UPDATE_LPE.contains(
            "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${EXPECTED_SCHEMA_VERSION}\"",
        ) && INIT_LPE.contains(
            "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${expected_schema_version}\"",
        ) && CHECK_LPE.contains(
            "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${expected_schema_version}\"",
        ),
        "update, initialization, and installation checks must all use the shared canonical shape guard"
    );
}

#[test]
fn canonical_schema_guard_rejects_incomplete_calendar_mail_classification_state() {
    assert_contains_all(
        "shared calendar-mail classification shape guard",
        INSTALL_COMMON,
        &[
            "('calendar_mail_classifications', 'r')",
            "('calendar_mail_classification_projections', 'r')",
            "calendar_mail_classification_shape_ok()",
            "calendar-mail-classification-shape.sql",
            "psql \"${database_url}\" -X -v ON_ERROR_STOP=1 -At -f \"${predicate_file}\"",
            "calendar_mail_classification_shape_ok \"${database_url}\"",
        ],
    );
    assert_contains_all(
        "read-only calendar-mail classification catalog predicate",
        CALENDAR_MAIL_CLASSIFICATION_SHAPE,
        &[
            "WITH relation_oids AS",
            "'mime_parts' AND table_row.relkind = 'r'",
            "'calendar_mail_classifications'",
            "'calendar_mail_classification_projections'",
            "'is_scheduling_body'",
            "'classification_generation'",
            "'requires_projection_rotation'",
            "'needs_reclassification'",
            "'applied_generation'",
            "pg_get_expr(default_row.adbin, default_row.adrelid) = 'false'",
            "pg_get_expr(default_row.adbin, default_row.adrelid) = '1'",
            "pg_get_expr(default_row.adbin, default_row.adrelid) = 'now()'",
            "mime_parts_scheduling_body_check",
            "lower(btrim(split_part(content_type, '';''::text, 1))) = ''text/calendar''::text",
            "mime_parts_one_scheduling_body_idx",
            "index_row.indisunique",
            "index_row.indisvalid",
            "index_row.indisready",
            "index_row.indislive",
            "pg_get_expr(index_row.indpred, index_row.indrelid, FALSE) = 'is_scheduling_body'",
            "calendar_mail_classifications_generation_check",
            "calendar_mail_classifications_metadata_shape_check",
            "constraint_row.definition LIKE '%metadata_json%request%IS TRUE%'",
            "constraint_row.definition LIKE '%metadata_json%response%IS TRUE%'",
            "constraint_row.definition LIKE '%jsonb_typeof%request%object%IS TRUE%'",
            "constraint_row.definition LIKE '%jsonb_typeof%response%object%IS TRUE%'",
            "calendar_mail_classifications_message_fkey",
            "calendar_mail_classifications_mime_part_fkey",
            "calendar_mail_classification_projections_generation_check",
            "calendar_mail_classification_projections_account_fkey",
            "calendar_mail_classification_projections_classification_fkey",
            "array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)",
            "constraint_row.confdeltype = 'c'",
            "constraint_row.convalidated",
            "THEN 1 ELSE 0 END;",
        ],
    );
    for forbidden in [
        "ALTER TABLE",
        "CREATE TABLE",
        "DROP TABLE",
        "INSERT INTO",
        "UPDATE public.",
        "DELETE FROM",
        "TRUNCATE",
        "LIKE '1%'",
        "LIKE 'false%'",
        "LIKE 'now()%'",
    ] {
        assert!(
            !CALENDAR_MAIL_CLASSIFICATION_SHAPE.contains(forbidden),
            "the installer calendar-mail classification predicate must remain read-only: {forbidden}"
        );
    }
    assert!(
        CALENDAR_MAIL_CLASSIFICATION_SHAPE
            .matches("array_agg(attribute_row.attname::text ORDER BY key_column.ordinality)")
            .count()
            == 2,
        "the installer guard must compare local and referenced FK columns as text arrays"
    );
    assert!(
        CALENDAR_MAIL_CLASSIFICATION_SHAPE
            .contains("pg_get_expr(default_row.adbin, default_row.adrelid) = '1'")
            && !CALENDAR_MAIL_CLASSIFICATION_SHAPE.contains("LIKE '1%"),
        "the installer guard must reject ALTER COLUMN classification_generation SET DEFAULT 100"
    );
    assert!(
        !CALENDAR_MAIL_CLASSIFICATION_SHAPE.contains("split_part(content_type, '';''::text, 2)"),
        "the installer guard must inspect the base media type rather than a parameter"
    );
    assert_before(
        UPDATE_LPE,
        "canonical_schema_shape_is_current \"${DATABASE_URL}\" \"${EXPECTED_SCHEMA_VERSION}\"",
        "systemctl stop \"${SERVICE_NAME}\"",
        "the updater must reject a same-label database with mutated calendar-mail state before stopping LPE",
    );
}

#[test]
fn canonical_schema_guard_validates_local_freebusy_trigger_and_property_semantics() {
    assert_contains_all(
        "shared LocalFreebusy projection guard",
        INSTALL_COMMON,
        &[
            "delegation_projection_shape_ok()",
            "expected_triggers(",
            "ARRAY['display_name', 'primary_email']::text[]",
            "position('INSERT INTO delegation_projection_state' IN procedure_row.prosrc) > 0",
            "position('revision = delegation_projection_state.revision + 1' IN procedure_row.prosrc) > 0",
            "PRIMARY KEY (tenant_id, account_id)",
            "constraint_row.confrelid = 'public.accounts'::regclass",
            "constraint_row.confdeltype = 'c'",
            "table_row.relname = expected.table_name",
            "procedure_row.proname = expected.function_name",
            "trigger_row.tgenabled = 'O'",
            "trigger_row.tgtype = expected.trigger_type",
            "trigger_row.tgnargs = 0",
            "mapi_auxiliary_shape_ok()",
            "PRIMARY KEY (tenant_id, account_id, object_kind, canonical_id, property_tag, property_type)",
            "ARRAY['tenant_id', 'account_id']::text[]",
            "ARRAY['tenant_id', 'id']::text[]",
        ],
    );
}

#[test]
fn active_source_key_index_guard_checks_semantics_in_validation_script() {
    assert_contains_all(
        "active SourceKey index helper",
        INSTALL_COMMON,
        &[
            "mapi_active_source_key_index_shape_ok()",
            "mapi_object_identities_active_source_key_uidx",
            "index_row.indisunique",
            "index_row.indisvalid",
            "index_row.indisready",
            "index_row.indislive",
            "pg_get_indexdef(index_row.indexrelid, 1, FALSE) = 'tenant_id'",
            "pg_get_expr(index_row.indpred, index_row.indrelid, FALSE)",
        ],
    );
    assert!(
        CHECK_LPE.contains("mapi_active_source_key_index_shape_ok \"${DATABASE_URL}\""),
        "check-lpe.sh must use the semantic active SourceKey index guard"
    );
}

#[test]
fn installation_scripts_validate_the_mapi_store_identity_singleton() {
    assert_contains_all(
        "MAPI store identity helper",
        INSTALL_COMMON,
        &[
            "mapi_store_identity_shape_ok()",
            "mapi_store_identity",
            "COUNT(*) FROM public.mapi_store_identity",
            "UNIQUE (mapi_global_counter)",
            "UNIQUE (mapi_object_id)",
        ],
    );
    assert!(
        INIT_LPE.contains("mapi_store_identity_shape_ok \"${DATABASE_URL}\"")
            && CHECK_LPE.contains("mapi_store_identity_shape_ok \"$DATABASE_URL\""),
        "init-schema.sh and check-lpe.sh must validate the MAPI store identity singleton"
    );
}

#[test]
fn installation_check_accepts_the_canonical_unconfigured_local_ai_state() {
    assert!(
        CHECK_LPE.contains("check_http_json_field \"$HTTP_BASE/health/local-ai\" '\"offline_only\":true'")
            && !CHECK_LPE.contains("'\"provider\":\"stub-local\"'"),
        "check-lpe.sh must validate the stable offline-only invariant instead of a configurable provider"
    );
}

#[test]
fn source_preflight_is_read_only_and_checks_known_050_shape_deltas() {
    assert_contains_all(
        "0.5.1 source preflight",
        PREFLIGHT,
        &[
            "BEGIN;",
            "SET TRANSACTION READ ONLY;",
            "SET LOCAL search_path = pg_catalog, public;",
            "installed_schema_version IS DISTINCT FROM '0.5.0-sql-v1'",
            "mapi_change_number",
            "predecessor_change_list",
            "mapi_object_identities_source_key_check",
            "mapi_object_identities_instance_key_check",
            "mapi_object_identities_active_source_key_uidx",
            "deleted_calendar_event",
            "calendar_events_owner_deleted_idx",
            "mapi_calendar_event_identity_moves",
            "octet_length(%change_key) >= 17",
            "next_global_counter",
            "mapi_special_folder_aliases",
            "PRIMARY KEY (tenant_id, account_id, alias_folder_id)",
            "FOREIGN KEY (tenant_id, account_id)",
            "mapi_navigation_shortcuts",
            "mapi_associated_config_messages",
            "ordinal_data_type NOT IN ('bigint', 'bytea')",
            "local_replica_table_count NOT IN (0, 2)",
            "unsupported 0.5.0-sql-v1 physical shape",
            "COMMIT;",
        ],
    );
    for forbidden in [
        "ALTER TABLE",
        "CREATE TABLE",
        "DROP TABLE",
        "INSERT INTO",
        "UPDATE public.",
        "DELETE FROM",
        "TRUNCATE",
    ] {
        assert!(
            !PREFLIGHT.contains(forbidden),
            "the 0.5.1 source preflight must remain read-only: {forbidden}"
        );
    }
}

#[test]
fn schema_transition_is_transactional_idempotent_and_version_bounded() {
    assert_contains_all(
        "0.5.1 schema transition",
        TRANSITION,
        &[
            "BEGIN;",
            "SET LOCAL search_path = pg_catalog, public;",
            "to_regclass('public.schema_metadata')",
            "installed_schema_version IS DISTINCT FROM '0.5.0-sql-v1'",
            "installed_schema_version IS DISTINCT FROM '0.5.1-sql'",
            "current_setting('lpe.schema_target_shape_validated', TRUE)",
            "validated update-lpe.sh session",
            "target_shape_ok",
            "mapi_local_replica_id_ranges",
            "mapi_local_replica_deleted_ranges",
            "mapi_navigation_shortcuts",
            "mapi_associated_config_messages_logical_idx",
            "mapi_object_identities_active_source_key_uidx",
            "LPE 0.5.1 target physical shape is incomplete",
            "DROP CONSTRAINT IF EXISTS schema_metadata_schema_version_check",
            "SET schema_version = '0.5.1-sql'",
            "ADD CONSTRAINT schema_metadata_schema_version_check",
            "CHECK (schema_version = '0.5.1-sql')",
            "RESET lpe.schema_target_shape_validated;",
            "COMMIT;",
        ],
    );
    assert_before(
        TRANSITION,
        "installed_schema_version IS DISTINCT FROM '0.5.0-sql-v1'",
        "INTO target_shape_ok",
        "the transition must validate the target shape after validating the source label",
    );
    assert_before(
        TRANSITION,
        "INTO target_shape_ok",
        "DROP CONSTRAINT IF EXISTS schema_metadata_schema_version_check",
        "the transition must validate physical state before changing metadata",
    );
    for forbidden in ["DROP TABLE", "DROP SCHEMA", "TRUNCATE"] {
        assert!(
            !TRANSITION.contains(forbidden),
            "the 0.5.1 transition must preserve canonical schema objects: {forbidden}"
        );
    }
}

fn assert_contains_all(label: &str, source: &str, needles: &[&str]) {
    for needle in needles {
        assert!(source.contains(needle), "{label} is missing {needle}");
    }
}

fn assert_before(source: &str, earlier: &str, later: &str, message: &str) {
    let earlier_offset = source
        .find(earlier)
        .unwrap_or_else(|| panic!("{message}: missing {earlier}"));
    let later_offset = source
        .find(later)
        .unwrap_or_else(|| panic!("{message}: missing {later}"));
    assert!(earlier_offset < later_offset, "{message}");
}
