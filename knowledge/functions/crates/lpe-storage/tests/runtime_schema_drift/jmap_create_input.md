---
type: Rust Function
title: jmap_create_input
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L659-L672
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_name_policy_storage_guards
---

# Signature

`fn jmap_create_input( account_id: Uuid, name: &str, parent_id: Option<Uuid>, ) -> JmapMailboxCreateInput`

# Called by

- [exercise_mailbox_name_policy_storage_guards](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_name_policy_storage_guards.md)