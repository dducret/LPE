---
type: Rust Function
title: access_plan_resolves_learned_special_folder_aliases
resource: crates/lpe-exchange/src/mapi/store_adapter/tests.rs#L198-L219
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/empty_session
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/single_rop_buffer
---

# Signature

`fn access_plan_resolves_learned_special_folder_aliases()`

# Calls

- [empty_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/empty_session.md)
- [record_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [plan_mapi_store_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [single_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/single_rop_buffer.md)