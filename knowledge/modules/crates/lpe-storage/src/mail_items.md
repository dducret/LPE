---
type: Rust Module
title: mail_items
resource: crates/lpe-storage/src/mail_items.rs#L1-L772
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-hashmap
  - external/anyhow-anyhow-bail-result
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-mapi-message-identity-rotate-active-mapi-message-identity-in-tx-auditentryinput-jmapemail-jmapemailfollowupupdate-storage
  - external/super-messageflagupdate
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [MessageFlagUpdate](../../../../classes/crates/lpe-storage/src/mail_items/MessageFlagUpdate.md)
- [into_followup_update](../../../../functions/crates/lpe-storage/src/mail_items/MessageFlagUpdate/into_followup_update.md)
- [update_message_flags](../../../../functions/crates/lpe-storage/src/mail_items/update_message_flags.md)
- [update_imap_flags](../../../../functions/crates/lpe-storage/src/mail_items/update_imap_flags.md)
- [expunge_imap_deleted](../../../../functions/crates/lpe-storage/src/mail_items/expunge_imap_deleted.md)
- [delete_custom_jmap_email](../../../../functions/crates/lpe-storage/src/mail_items/delete_custom_jmap_email.md)
- [delete_jmap_email](../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email.md)
- [delete_jmap_email_from_mailbox](../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_from_mailbox.md)
- [delete_jmap_email_memberships](../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships.md)
- [recoverable_created_by_protocol](../../../../functions/crates/lpe-storage/src/mail_items/recoverable_created_by_protocol.md)
- [message_flag_update_projects_followup_flag_status](../../../../functions/crates/lpe-storage/src/mail_items/message_flag_update_projects_followup_flag_status.md)

# Imports

- `std::collections::HashMap`
- `anyhow::{anyhow, bail, Result}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    mapi_message_identity::rotate_active_mapi_message_identity_in_tx, AuditEntryInput, JmapEmail,
    JmapEmailFollowupUpdate, Storage,
}`
- `super::MessageFlagUpdate`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)