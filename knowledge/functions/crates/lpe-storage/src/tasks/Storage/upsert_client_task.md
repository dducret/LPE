---
type: Rust Method
title: upsert_client_task
resource: crates/lpe-storage/src/tasks.rs#L22-L271
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/normalize_task_status
  - functions/crates/lpe-storage/src/tasks/parse_task_timestamp
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids
  - functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_task_access_change
  - functions/crates/lpe-storage/src/tasks/types/map_task
---

# Signature

`pub async fn upsert_client_task(&self, input: UpsertClientTaskInput) -> Result<ClientTask>`

# Calls

- [normalize_task_status](../../../../../../functions/crates/lpe-storage/src/util/normalize_task_status.md)
- [parse_task_timestamp](../../../../../../functions/crates/lpe-storage/src/tasks/parse_task_timestamp.md)
- [fetch_client_tasks_by_ids](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [fetch_task_lists_by_ids](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids.md)
- [fetch_task_lists](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_task_list](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_task_access_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_task_access_change.md)
- [map_task](../../../../../../functions/crates/lpe-storage/src/tasks/types/map_task.md)