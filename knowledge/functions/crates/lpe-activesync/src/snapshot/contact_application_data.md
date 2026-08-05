---
type: Rust Function
title: contact_application_data
resource: crates/lpe-activesync/src/snapshot.rs#L188-L226
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/message/split_name
  - functions/crates/lpe-activesync/src/snapshot/push_text
  - functions/crates/lpe-activesync/src/snapshot/push_body
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes
---

# Signature

`pub(crate) fn contact_application_data(contact: &ClientContact) -> Value`

# Calls

- [split_name](../../../../../functions/crates/lpe-activesync/src/message/split_name.md)
- [push_text](../../../../../functions/crates/lpe-activesync/src/snapshot/push_text.md)
- [push_body](../../../../../functions/crates/lpe-activesync/src/snapshot/push_body.md)

# Called by

- [fetch_collection_nodes](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes.md)