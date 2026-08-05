---
type: Rust Function
title: update_message_flags
resource: crates/lpe-storage/src/mail_items.rs#L35-L81
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/mail_items/MessageFlagUpdate/into_followup_update
  called_by:
  - functions/crates/lpe-activesync/src/store/Storage/activesyncstore/update_jmap_email_flags
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_flags
---

# Signature

`pub async fn update_message_flags( storage: &Storage, account_id: Uuid, message_id: Uuid, update: MessageFlagUpdate, audit: AuditEntryInput, ) -> Result<JmapEmail>`

# Calls

- [tenant_id_for_account_id](../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [into_followup_update](../../../../../functions/crates/lpe-storage/src/mail_items/MessageFlagUpdate/into_followup_update.md)

# Called by

- [update_jmap_email_flags](../../../../../functions/crates/lpe-activesync/src/store/Storage/activesyncstore/update_jmap_email_flags.md)
- [update_jmap_email_flags](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_flags.md)