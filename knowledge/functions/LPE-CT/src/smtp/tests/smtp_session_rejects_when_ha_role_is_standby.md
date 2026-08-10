---
type: Rust Function
title: smtp_session_rejects_when_ha_role_is_standby
resource: LPE-CT/src/smtp/tests.rs#L2000-L2032
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains
  - functions/crates/lpe-storage/src/core/Storage/connect
---

# Signature

`async fn smtp_session_rejects_when_ha_role_is_standby()`

# Calls

- [env_test_lock](../../../../../functions/LPE-CT/src/env_test_lock.md)
- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [handle_smtp_session](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [runtime_store_with_accepted_domains](../../../../../functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)