---
type: Rust Function
title: replace_attachment_custom_properties_in_tx
resource: crates/lpe-storage/src/attachments.rs#L1087-L1114
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx
---

# Signature

`async fn replace_attachment_custom_properties_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, attachment_id: Uuid, values: &[MapiEventCustomPropertyValue], ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [apply_mapi_event_attachment_changes_in_tx](../../../../../functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx.md)