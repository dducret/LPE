---
type: Rust Function
title: mapi_default_calendar_folder_identity_is_persisted
resource: crates/lpe-exchange/src/tests/mod.rs#L2313-L2378
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn mapi_default_calendar_folder_identity_is_persisted()`

# Calls

- [load_mapi_mail_store](../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [virtual_special_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)