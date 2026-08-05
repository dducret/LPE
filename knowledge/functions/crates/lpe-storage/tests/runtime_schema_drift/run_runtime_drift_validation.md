---
type: Rust Function
title: run_runtime_drift_validation
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L80-L295
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/tests/runtime_schema_drift/assert_schema_metadata
  - functions/crates/lpe-storage/tests/runtime_schema_drift/seed_platform_tenant
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_blob_reference_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/seed_mailbox_fixture
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_local_replica_range_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_outlook_cache_fidelity_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_change_log_cursor_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_special_folder_alias_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_inbound_mime_canonical_body_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_name_policy_storage_guards
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_managed_retention_folder_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_jmap_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_cancellation_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_index_plan_paths
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_cross_protocol_interoperability_gate
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_canonical_identity_allocation
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_canonical_search_folder_and_rule_replay
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_replica_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_permission_replay_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_per_user_replay_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_activesync_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_notes_journal_reminder_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_pst_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_move_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_delete_cross_protocol_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_trash_purge_cross_protocol_path
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_trash_purge_retention_guard
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_dashboard_path
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/schema_sql_matches_representative_runtime_paths_when_database_is_enabled
---

# Signature

`async fn run_runtime_drift_validation(pool: &PgPool) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [assert_schema_metadata](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/assert_schema_metadata.md)
- [seed_platform_tenant](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/seed_platform_tenant.md)
- [exercise_blob_reference_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_blob_reference_constraints.md)
- [exercise_admin_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_path.md)
- [seed_mailbox_fixture](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/seed_mailbox_fixture.md)
- [exercise_mapi_local_replica_range_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_local_replica_range_constraints.md)
- [exercise_mapi_outlook_cache_fidelity_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_outlook_cache_fidelity_constraints.md)
- [exercise_change_log_cursor_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_change_log_cursor_constraints.md)
- [exercise_mapi_special_folder_alias_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_special_folder_alias_constraints.md)
- [exercise_mailbox_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_path.md)
- [exercise_inbound_mime_canonical_body_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_inbound_mime_canonical_body_path.md)
- [exercise_mailbox_name_policy_storage_guards](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_name_policy_storage_guards.md)
- [exercise_managed_retention_folder_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_managed_retention_folder_path.md)
- [exercise_submission_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_path.md)
- [exercise_jmap_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_jmap_path.md)
- [exercise_submission_cancellation_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_cancellation_path.md)
- [exercise_index_plan_paths](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_index_plan_paths.md)
- [exercise_mapi_cross_protocol_interoperability_gate](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_cross_protocol_interoperability_gate.md)
- [exercise_canonical_identity_allocation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_canonical_identity_allocation.md)
- [exercise_canonical_search_folder_and_rule_replay](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_canonical_search_folder_and_rule_replay.md)
- [exercise_public_folder_replica_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_replica_path.md)
- [exercise_public_folder_permission_replay_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_permission_replay_path.md)
- [exercise_public_folder_per_user_replay_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_per_user_replay_path.md)
- [exercise_custom_calendar_grant_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path.md)
- [exercise_activesync_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_activesync_path.md)
- [exercise_notes_journal_reminder_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_notes_journal_reminder_path.md)
- [exercise_pst_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_pst_path.md)
- [exercise_mailbox_move_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_move_path.md)
- [exercise_mapi_delete_cross_protocol_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_delete_cross_protocol_path.md)
- [exercise_mapi_trash_purge_cross_protocol_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_trash_purge_cross_protocol_path.md)
- [exercise_mapi_trash_purge_retention_guard](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_trash_purge_retention_guard.md)
- [exercise_admin_dashboard_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_dashboard_path.md)

# Called by

- [schema_sql_matches_representative_runtime_paths_when_database_is_enabled](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/schema_sql_matches_representative_runtime_paths_when_database_is_enabled.md)