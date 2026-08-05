---
type: Rust Method
title: new
resource: crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity.rs#L180-L207
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`pub(crate) fn new( mailboxes: Vec<JmapMailbox>, emails: Vec<JmapEmail>, attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>, contact_collections: Vec<CollaborationCollection>, calendar_collections: Vec<CollaborationCollection>, task_collections: Vec<CollaborationCollection>, contacts: Vec<AccessibleContact>, events: Vec<AccessibleEvent>, tasks: Vec<ClientTask>, folder_permissions: Vec<MapiFolderPermission>, ) -> Self`

# Calls

- [build](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)
- [expect](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)