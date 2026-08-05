---
type: Rust Function
title: header_policy_key
resource: crates/lpe-activesync/src/service/provisioning.rs#L157-L164
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) fn header_policy_key(headers: &HeaderMap) -> Option<String>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_parsed_request](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)