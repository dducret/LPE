---
type: Rust Function
title: protocol_version
resource: crates/lpe-activesync/src/auth.rs#L6-L14
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(crate) fn protocol_version(headers: &HeaderMap) -> String`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_parsed_request](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)