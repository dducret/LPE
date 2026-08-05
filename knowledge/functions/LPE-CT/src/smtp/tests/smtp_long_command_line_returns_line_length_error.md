---
type: Rust Function
title: smtp_long_command_line_returns_line_length_error
resource: LPE-CT/src/smtp/tests.rs#L507-L534
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

`async fn smtp_long_command_line_returns_line_length_error()`

# Calls

- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [runtime_store_with_accepted_domains](../../../../../functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)