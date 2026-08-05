---
type: Rust Method
title: system_role_for_display_name
resource: crates/lpe-domain/src/mailbox_name.rs#L206-L211
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name
  called_by:
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path
  - functions/crates/lpe-imap/src/render/mailbox_name_matches
  - functions/crates/lpe-storage/src/util/system_mailbox_role_for_display_name
---

# Signature

`pub fn system_role_for_display_name(value: &str) -> Option<&'static str>`

# Calls

- [for_display_name](../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/for_display_name.md)

# Called by

- [handle_folder_create](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_update](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
- [parse_mailbox_path](../../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path.md)
- [mailbox_name_matches](../../../../../../functions/crates/lpe-imap/src/render/mailbox_name_matches.md)
- [system_mailbox_role_for_display_name](../../../../../../functions/crates/lpe-storage/src/util/system_mailbox_role_for_display_name.md)