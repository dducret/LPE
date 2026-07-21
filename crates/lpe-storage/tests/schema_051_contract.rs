const SCHEMA: &str = include_str!("../sql/schema.sql");
const PREFLIGHT: &str =
    include_str!("../sql/updates/0.5.0-sql-v1-to-0.5.1-sql-preflight.sql");
const TRANSITION: &str = include_str!("../sql/updates/0.5.0-sql-v1-to-0.5.1-sql.sql");
const UPDATE_LPE: &str = include_str!("../../../installation/debian-trixie/update-lpe.sh");

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
        "systemctl stop \"${SERVICE_NAME}\"",
        "the updater must reject unsupported 0.5.0 shapes before stopping LPE",
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
        "psql \"${DATABASE_URL}\" -X -v ON_ERROR_STOP=1 -f \"${SCHEMA_051_UPDATE_FILE}\"",
        "the updater must validate the target shape before committing the 0.5.1 label",
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
            "deleted_calendar_event",
            "calendar_events_owner_deleted_idx",
            "mapi_calendar_event_identity_moves",
            "octet_length(%change_key) >= 17",
            "next_global_counter",
            "mapi_special_folder_aliases",
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
            "DROP CONSTRAINT IF EXISTS schema_metadata_schema_version_check",
            "SET schema_version = '0.5.1-sql'",
            "ADD CONSTRAINT schema_metadata_schema_version_check",
            "CHECK (schema_version = '0.5.1-sql')",
            "COMMIT;",
        ],
    );
    assert_before(
        TRANSITION,
        "installed_schema_version IS DISTINCT FROM '0.5.0-sql-v1'",
        "DROP CONSTRAINT IF EXISTS schema_metadata_schema_version_check",
        "the transition must reject unsupported versions before changing metadata",
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
