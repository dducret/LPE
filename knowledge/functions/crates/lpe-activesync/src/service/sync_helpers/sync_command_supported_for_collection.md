---
type: Rust Function
title: sync_command_supported_for_collection
resource: crates/lpe-activesync/src/service/sync_helpers.rs#L73-L84
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/drafts_collection
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  called_by:
  - functions/crates/lpe-activesync/src/service/sync_helpers/sync_collection_has_unsupported_command
---

# Signature

`fn sync_command_supported_for_collection(command: &str, collection: &CollectionDefinition) -> bool`

# Calls

- [drafts_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/drafts_collection.md)
- [mail_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)

# Called by

- [sync_collection_has_unsupported_command](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_collection_has_unsupported_command.md)