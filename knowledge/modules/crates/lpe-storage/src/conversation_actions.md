---
type: Rust Module
title: conversation_actions
resource: crates/lpe-storage/src/conversation_actions.rs#L1-L389
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-json-json
  - external/sqlx-fromrow-row
  - external/uuid-uuid
  - external/crate-auditentryinput-canonicalchangecategory-jmapemailfollowupupdate-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [ConversationAction](../../../../classes/crates/lpe-storage/src/conversation_actions/ConversationAction.md)
- [UpsertConversationActionInput](../../../../classes/crates/lpe-storage/src/conversation_actions/UpsertConversationActionInput.md)
- [ConversationActionRow](../../../../classes/crates/lpe-storage/src/conversation_actions/ConversationActionRow.md)
- [fetch_conversation_actions](../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/fetch_conversation_actions.md)
- [fetch_conversation_actions_by_ids](../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/fetch_conversation_actions_by_ids.md)
- [upsert_conversation_action](../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/upsert_conversation_action.md)
- [delete_conversation_action](../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/delete_conversation_action.md)
- [apply_conversation_actions_to_jmap_email](../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/apply_conversation_actions_to_jmap_email.md)
- [map_conversation_action](../../../../functions/crates/lpe-storage/src/conversation_actions/map_conversation_action.md)

# Imports

- `anyhow::{bail, Result}`
- `serde_json::json`
- `sqlx::{FromRow, Row}`
- `uuid::Uuid`
- `crate::{AuditEntryInput, CanonicalChangeCategory, JmapEmailFollowupUpdate, Storage}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)