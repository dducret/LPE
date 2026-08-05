---
type: Rust Function
title: ensure_supported_protocol_version
resource: crates/lpe-activesync/src/auth.rs#L16-L22
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(crate) fn ensure_supported_protocol_version(protocol_version: &str) -> Result<()>`

# Called by

- [handle_parsed_request](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)