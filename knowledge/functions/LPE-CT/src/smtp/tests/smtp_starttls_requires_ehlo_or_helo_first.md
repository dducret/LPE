---
type: Rust Function
title: smtp_starttls_requires_ehlo_or_helo_first
resource: LPE-CT/src/smtp/tests.rs#L1540-L1566
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
---

# Signature

`async fn smtp_starttls_requires_ehlo_or_helo_first()`

# Calls

- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [runtime_store_with_accepted_domains](../../../../../functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)