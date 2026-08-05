---
type: Rust Function
title: ping_change_categories
resource: crates/lpe-activesync/src/service/ping.rs#L336-L357
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`fn ping_change_categories( monitored: &[(CollectionDefinition, StoredSyncState)], ) -> Vec<CanonicalChangeCategory>`

# Calls

- [mail_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_ping](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)