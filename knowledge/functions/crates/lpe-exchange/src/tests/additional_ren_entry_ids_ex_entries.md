---
type: Rust Function
title: additional_ren_entry_ids_ex_entries
resource: crates/lpe-exchange/src/tests/mod.rs#L12779-L12811
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_logon_advertises_openable_additional_ren_entryids_ex
---

# Signature

`fn additional_ren_entry_ids_ex_entries(value: &[u8]) -> Vec<(u16, u64)>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [mapi_over_http_logon_advertises_openable_additional_ren_entryids_ex](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_logon_advertises_openable_additional_ren_entryids_ex.md)