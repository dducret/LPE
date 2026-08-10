---
type: Rust Function
title: router
resource: LPE-CT/src/main.rs#L710-L832
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/LPE-CT/src/outbound_handoff_body_limit
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/LPE-CT/src/host_logs/delete
---

# Signature

`fn router(state: AppState) -> Router`

# Calls

- [expect](../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [outbound_handoff_body_limit](../../../functions/LPE-CT/src/outbound_handoff_body_limit.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [delete](../../../functions/LPE-CT/src/host_logs/delete.md)