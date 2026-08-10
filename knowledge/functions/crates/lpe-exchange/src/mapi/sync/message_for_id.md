---
type: Rust Function
title: message_for_id
resource: crates/lpe-exchange/src/mapi/sync.rs#L1388-L1398
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches
  - functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder
---

# Signature

`pub(in crate::mapi) fn message_for_id<'a>( folder_id: u64, message_id: u64, mailboxes: &[JmapMailbox], emails: &'a [JmapEmail], ) -> Option<&'a JmapEmail>`

# Calls

- [mapi_item_id_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches.md)
- [email_matches_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder.md)