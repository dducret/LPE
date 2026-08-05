---
type: Rust Module
title: recoverable_items
resource: crates/lpe-storage/src/recoverable_items.rs#L1-L436
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/serde-serialize
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-auditentryinput-jmapemail-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [RecoverableItem](../../../../classes/crates/lpe-storage/src/recoverable_items/RecoverableItem.md)
- [list_recoverable_items](../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/list_recoverable_items.md)
- [restore_recoverable_item](../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/restore_recoverable_item.md)
- [purge_recoverable_item](../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/purge_recoverable_item.md)
- [rebuild_mail_search_document_in_tx](../../../../functions/crates/lpe-storage/src/recoverable_items/Storage/rebuild_mail_search_document_in_tx.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `serde::Serialize`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{AuditEntryInput, JmapEmail, Storage}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)