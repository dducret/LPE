---
type: Rust Function
title: record_mapi_folder_purge_metrics
resource: crates/lpe-exchange/src/mapi.rs#L159-L171
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/hard_delete_public_folder_contents
---

# Signature

`pub(crate) fn record_mapi_folder_purge_metrics( attempted: usize, succeeded: usize, failed: usize, partial_completion: bool, )`

# Called by

- [hard_delete_folder_contents](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents.md)
- [hard_delete_mailbox_tree_contents](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents.md)
- [hard_delete_public_folder_contents](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/hard_delete_public_folder_contents.md)