---
type: Rust Function
title: ensure_condstore_fetch_attributes
resource: crates/lpe-imap/src/messages.rs#L460-L469
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
---

# Signature

`fn ensure_condstore_fetch_attributes(requested: &mut FetchAttributes, changed_since: Option<u64>)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_fetch](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)