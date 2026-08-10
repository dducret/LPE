---
type: Rust Method
title: create_jmap_mailbox
resource: crates/lpe-storage/src/mailboxes.rs#L468-L638
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-storage/src/util/system_mailbox_role_for_display_name
  - functions/crates/lpe-storage/src/util/canonical_system_mailbox_display_name
  - functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_parent_valid_in_tx
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
---

# Signature

`pub async fn create_jmap_mailbox( &self, input: JmapMailboxCreateInput, audit: AuditEntryInput, ) -> Result<JmapMailbox>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [system_mailbox_role_for_display_name](../../../../../../functions/crates/lpe-storage/src/util/system_mailbox_role_for_display_name.md)
- [canonical_system_mailbox_display_name](../../../../../../functions/crates/lpe-storage/src/util/canonical_system_mailbox_display_name.md)
- [ensure_mailbox](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_mailbox.md)
- [set_mailbox_subscription_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription_in_tx.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [ensure_mailbox_parent_valid_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_parent_valid_in_tx.md)
- [ensure_mailbox_name_available_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx.md)
- [allocate_mail_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_mail_modseq_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [affected_mail_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/affected_mail_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)