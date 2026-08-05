---
type: Rust Function
title: ensure_write
resource: crates/lpe-storage/src/public_folders/types.rs#L192-L198
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item
---

# Signature

`pub(crate) fn ensure_write(access: PublicFolderAccess) -> Result<()>`

# Called by

- [upsert_public_folder_item](../../../../../../functions/crates/lpe-storage/src/public_folders/Storage/upsert_public_folder_item.md)