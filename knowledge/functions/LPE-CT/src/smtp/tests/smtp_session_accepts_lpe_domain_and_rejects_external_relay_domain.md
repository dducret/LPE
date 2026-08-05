---
type: Rust Function
title: smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain
resource: LPE-CT/src/smtp/tests.rs#L755-L834
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/crates/lpe-storage/src/core/Storage/connect
---

# Signature

`async fn smtp_session_accepts_lpe_domain_and_rejects_external_relay_domain()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_store_with_accepted_domains](../../../../../functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains.md)
- [handle_smtp_session](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)