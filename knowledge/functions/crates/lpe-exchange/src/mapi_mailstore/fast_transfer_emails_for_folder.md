---
type: Rust Function
title: fast_transfer_emails_for_folder
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L979-L996
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_email_matches_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content
---

# Signature

`fn fast_transfer_emails_for_folder( folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> Vec<JmapEmail>`

# Calls

- [fast_transfer_email_matches_folder](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_email_matches_folder.md)

# Called by

- [write_fast_transfer_folder_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content.md)