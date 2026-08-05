---
type: Rust Function
title: smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params
resource: LPE-CT/src/smtp/tests.rs#L406-L456
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

`async fn smtp_rcpt_to_rejects_malformed_paths_and_unsupported_params()`

# Calls

- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [runtime_store_with_accepted_domains](../../../../../functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)