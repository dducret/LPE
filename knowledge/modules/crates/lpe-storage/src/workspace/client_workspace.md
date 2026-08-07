---
type: Rust Module
title: client_workspace
resource: crates/lpe-storage/src/workspace/client_workspace.rs#L1-L306
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/uuid-uuid
  - external/crate-accessiblecontact-accessibleevent-clientattachment-clientattachmentrow-clientmessagerow-storage-attachments
  - external/super-clientmessage-clientworkspace
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [fetch_client_workspace](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/Storage/fetch_client_workspace.md)
- [body_paragraphs](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/body_paragraphs.md)
- [client_folder](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/client_folder.md)
- [client_message_tags](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/client_message_tags.md)
- [format_size](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/format_size.md)
- [client_event_from_accessible](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/client_event_from_accessible.md)
- [client_contact_from_accessible](../../../../../functions/crates/lpe-storage/src/workspace/client_workspace/client_contact_from_accessible.md)

# Imports

- `anyhow::Result`
- `uuid::Uuid`
- `crate::{
    AccessibleContact, AccessibleEvent, ClientAttachment, ClientAttachmentRow, ClientMessageRow,
    Storage, attachments,
}`
- `super::{ClientMessage, ClientWorkspace}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)