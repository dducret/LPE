---
type: Rust Function
title: sync_state_folder_kind
resource: crates/lpe-exchange/src/service/ews/folders.rs#L588-L604
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind
---

# Signature

`fn sync_state_folder_kind(sync_state: &str) -> Option<FolderKind>`

# Called by

- [requested_folder_kind](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind.md)