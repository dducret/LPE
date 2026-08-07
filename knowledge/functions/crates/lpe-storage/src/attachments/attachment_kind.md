---
type: Rust Function
title: attachment_kind
resource: crates/lpe-storage/src/attachments.rs#L1196-L1213
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/attachments/attachment_extension_label
  - functions/crates/lpe-storage/src/attachments/media_type_label
  called_by:
  - functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace
---

# Signature

`pub(crate) fn attachment_kind(media_type: &str, name: &str) -> String`

# Calls

- [attachment_extension_label](../../../../../functions/crates/lpe-storage/src/attachments/attachment_extension_label.md)
- [media_type_label](../../../../../functions/crates/lpe-storage/src/attachments/media_type_label.md)

# Called by

- [fetch_client_workspace](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace.md)