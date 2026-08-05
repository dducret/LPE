---
type: Rust Function
title: api_request_exceeds_call_limit
resource: crates/lpe-jmap/src/service/helpers.rs#L3-L5
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/service/api_handler
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) fn api_request_exceeds_call_limit(request: &JmapApiRequest) -> bool`

# Called by

- [api_handler](../../../../../../functions/crates/lpe-jmap/src/service/api_handler.md)
- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)