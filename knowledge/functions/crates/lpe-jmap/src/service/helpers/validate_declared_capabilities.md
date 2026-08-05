---
type: Rust Function
title: validate_declared_capabilities
resource: crates/lpe-jmap/src/service/helpers.rs#L275-L282
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/is_supported_capability
  called_by:
  - functions/crates/lpe-jmap/src/service/api_handler
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) fn validate_declared_capabilities(request: &JmapApiRequest) -> Result<()>`

# Calls

- [is_supported_capability](../../../../../../functions/crates/lpe-jmap/src/service/helpers/is_supported_capability.md)

# Called by

- [api_handler](../../../../../../functions/crates/lpe-jmap/src/service/api_handler.md)
- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)