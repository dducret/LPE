const SCHEMA: &str = include_str!("../sql/schema.sql");
const PREFLIGHT: &str = include_str!("../sql/updates/0.5.0-sql-v1-to-0.5.1-sql-preflight.sql");
const TRANSITION: &str = include_str!("../sql/updates/0.5.0-sql-v1-to-0.5.1-sql.sql");
const UPDATE_LPE: &str = include_str!("../../../installation/debian-trixie/update-lpe.sh");
const CHECK_LPE: &str = include_str!("../../../installation/debian-trixie/check-lpe.sh");
const INSTALL_COMMON: &str =
    include_str!("../../../installation/debian-trixie/lib/install-common.sh");

#[test]
fn canonical_schema_uses_051_release_label() {
    assert!(
        SCHEMA.contains("schema_version = '0.5.1-sql'")
            && SCHEMA.contains("VALUES (TRUE, '0.5.1-sql')"),
        "the canonical schema must use the exact 0.5.1-sql release label"
    );
    assert!(
        !SCHEMA.contains("0.5.0-sql-v1"),
        "the canonical 0.5.1 schema must not retain the old release label"
    );
}

#[test]
fn update_script_preflights_050_then_validates_before_relabeling_051() {
    assert_contains_all(
        "update-lpe.sh",
        UPDATE_LPE,
        &[
            "SELECT schema_version FROM public.schema_metadata WHERE singleton = TRUE",
            "SOURCE_SCHEMA_VERSION=\"0.5.0-sql-v1\"",
            "SCHEMA_051_PREFLIGHT_FILE",
            "0.5.0-sql-v1-to-0.5.1-sql-preflight.sql",
            "OUTLOOK_CACHE_FIDELITY_UPDATE_FILE",
            "0.5.0-sql-v1-outlook-cache-fidelity.sql",
            "SCHEMA_051_UPDATE_FILE",
            "0.5.0-sql-v1-to-0.5.1-sql.sql",
            "SET lpe.schema_target_shape_validated = '0.5.1-sql'",
            "mapi_active_source_key_index_shape_ok",
            "Database schema ${EXPECTED_SCHEMA_VERSION} is current",
        ],
    );
    for forbidden in [
        "CREATE TABLE",
        "ALTER TABLE",
        "UPDATE public.",
        "DELETE FROM public.",
    ] {
        assert!(
            !UPDATE_LPE.contains(forbidden),
            "update-lpe.sh must keep schema mutations in reviewed SQL files: {forbidden}"
        );
    }
    assert_before(
        UPDATE_LPE,
        "case \"${INSTALLED_SCHEMA_VERSION}\" in",
        "psql \"${DATABASE_URL}\" -X -v ON_ERROR_STOP=1 -f \"${SCHEMA_051_PREFLIGHT_FILE}\"",
        "the updater must reject unsupported labels before the physical preflight",
    );
    assert_before(
        UPDATE_LPE,
        "psql \"${DATABASE_URL}\" -X -v ON_ERROR_STOP=1 -f \"${SCHEMA_051_PREFLIGHT_FILE}\"",
        "SOURCE_LOCAL_REPLICA_TABLE_COUNT",
        "the source preflight must precede local-replica applicability checks",
    );
    assert_before(
        UPDATE_LPE,
        "SOURCE_LOCAL_REPLICA_RANGE_SHAPE_OK",
        "systemctl stop \"${SERVICE_NAME}\"",
        "the updater must reject unrecoverable local-replica shapes before stopping LPE",
    );
    assert_before(
        UPDATE_LPE,
        "systemctl stop \"${SERVICE_NAME}\"",
        "psql \"${DATABASE_URL}\" -X -v ON_ERROR_STOP=1 -f \"${OUTLOOK_CACHE_FIDELITY_UPDATE_FILE}\"",
        "the updater must stop LPE before applying the physical update",
    );
    assert_before(
        UPDATE_LPE,
        "psql \"${DATABASE_URL}\" -X -v ON_ERROR_STOP=1 -f \"${OUTLOOK_CACHE_FIDELITY_UPDATE_FILE}\"",
        "MAPI_LOCAL_REPLICA_RANGE_SHAPE_OK",
        "the physical update must precede target-shape validation",
    );
    assert_before(
        UPDATE_LPE,
        "MAPI_SPECIAL_FOLDER_ALIAS_SHAPE_OK",
        "-f \"${SCHEMA_051_UPDATE_FILE}\"",
        "the updater must validate the target shape before committing the 0.5.1 label",
    );
    assert_before(
        UPDATE_LPE,
        "Schema transition finished with ${INSTALLED_SCHEMA_VERSION}",
        "if [[ \"${MIGRATE_SCHEMA_FROM_050}\" == \"false\" ]]",
        "a current 0.5.1 database must pass read-only guards before LPE is stopped",
    );
}

#[test]
fn active_source_key_index_guard_checks_semantics_in_update_and_validation_scripts() {
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
        UPDATE_LPE.contains("mapi_active_source_key_index_shape_ok \"${DATABASE_URL}\"")
            && CHECK_LPE.contains("mapi_active_source_key_index_shape_ok \"${DATABASE_URL}\""),
        "update-lpe.sh and check-lpe.sh must both use the semantic active SourceKey index guard"
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
