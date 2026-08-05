---
type: Rust Method
title: fetch_collection_nodes
resource: crates/lpe-activesync/src/service.rs#L745-L816
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  - functions/crates/lpe-activesync/src/snapshot/email_application_data
  - functions/crates/lpe-activesync/src/service/Pipe/pipe
  - functions/crates/lpe-activesync/src/snapshot/contact_application_data
  - functions/crates/lpe-activesync/src/snapshot/calendar_application_data
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/build_commands
---

# Signature

`async fn fetch_collection_nodes( &self, account_id: Uuid, collection: &CollectionDefinition, page_changes: &[SnapshotChange], body_preference: &BodyPreference, ) -> Result<HashMap<String, WbxmlNode>>`

# Calls

- [mail_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)
- [email_application_data](../../../../../../functions/crates/lpe-activesync/src/snapshot/email_application_data.md)
- [pipe](../../../../../../functions/crates/lpe-activesync/src/service/Pipe/pipe.md)
- [contact_application_data](../../../../../../functions/crates/lpe-activesync/src/snapshot/contact_application_data.md)
- [calendar_application_data](../../../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)

# Called by

- [build_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/build_commands.md)