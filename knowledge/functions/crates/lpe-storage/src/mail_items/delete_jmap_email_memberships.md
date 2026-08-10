---
type: Rust Function
title: delete_jmap_email_memberships
resource: crates/lpe-storage/src/mail_items.rs#L503-L730
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/mail_items/recoverable_created_by_protocol
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
  called_by:
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email_from_mailbox
---

# Signature

`async fn delete_jmap_email_memberships( storage: &Storage, account_id: Uuid, mailbox_id_filter: Option<Uuid>, message_id: Uuid, audit: AuditEntryInput, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [recoverable_created_by_protocol](../../../../../functions/crates/lpe-storage/src/mail_items/recoverable_created_by_protocol.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [allocate_mail_modseq_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [affected_mail_principals_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [recalculate_mailbox_counts_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/recalculate_mailbox_counts_in_tx.md)
- [insert_audit](../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [emit_mail_change](../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)

# Called by

- [delete_jmap_email](../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email.md)
- [delete_jmap_email_from_mailbox](../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_from_mailbox.md)