---
type: Rust Method
title: known_unsupported_name_for_str
resource: crates/lpe-activesync/src/protocol.rs#L140-L149
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/from_name
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(crate) fn known_unsupported_name_for_str(value: &str) -> Option<&'static str>`

# Called by

- [from_name](../../../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/from_name.md)
- [handle_parsed_request](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)