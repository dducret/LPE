---
type: Rust Function
title: snapshot_email_belongs_to_folder
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1430-L1436
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/email_role_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_message_counts_for_folder
---

# Signature

`fn snapshot_email_belongs_to_folder(email: &JmapEmail, folder_id: u64) -> bool`

# Calls

- [email_role_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/email_role_folder_id.md)

# Called by

- [snapshot_message_counts_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_message_counts_for_folder.md)