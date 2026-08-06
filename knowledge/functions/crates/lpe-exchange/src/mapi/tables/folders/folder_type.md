---
type: Rust Function
title: folder_type
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L43-L49
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags
---

# Signature

`pub(super) fn folder_type(mailbox: &JmapMailbox) -> u32`

# Called by

- [serialize_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [hierarchy_row_folder_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_folder_flags.md)