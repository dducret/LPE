---
type: Rust Function
title: outbound_route_without_smart_host_uses_direct_mx_default
resource: LPE-CT/src/smtp/tests.rs#L2217-L2224
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/outbound_policy/resolve_outbound_route
  - functions/LPE-CT/src/smtp/tests/outbound_request
---

# Signature

`fn outbound_route_without_smart_host_uses_direct_mx_default()`

# Calls

- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [resolve_outbound_route](../../../../../functions/LPE-CT/src/smtp/outbound_policy/resolve_outbound_route.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)