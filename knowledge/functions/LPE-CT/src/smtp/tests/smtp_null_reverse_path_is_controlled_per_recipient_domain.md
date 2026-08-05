---
type: Rust Function
title: smtp_null_reverse_path_is_controlled_per_recipient_domain
resource: LPE-CT/src/smtp/tests.rs#L686-L752
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

`async fn smtp_null_reverse_path_is_controlled_per_recipient_domain()`

# Calls

- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [runtime_store_with_accepted_domains](../../../../../functions/LPE-CT/src/smtp/tests/runtime_store_with_accepted_domains.md)
- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)