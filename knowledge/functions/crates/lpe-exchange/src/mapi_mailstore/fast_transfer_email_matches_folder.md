---
type: Rust Function
title: fast_transfer_email_matches_folder
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1001-L1027
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_emails_for_folder
---

# Signature

`fn fast_transfer_email_matches_folder( email: &JmapEmail, folder_id: u64, mailboxes: &[JmapMailbox], ) -> bool`

# Calls

- [virtual_special_folder_metadata](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata.md)
- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [mapi_folder_id_for_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)

# Called by

- [fast_transfer_emails_for_folder](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_emails_for_folder.md)