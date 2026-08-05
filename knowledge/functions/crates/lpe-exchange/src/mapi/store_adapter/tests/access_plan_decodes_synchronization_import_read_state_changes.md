---
type: Rust Function
title: access_plan_decodes_synchronization_import_read_state_changes
resource: crates/lpe-exchange/src/mapi/store_adapter/tests.rs#L1265-L1296
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/single_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/empty_session
---

# Signature

`fn access_plan_decodes_synchronization_import_read_state_changes()`

# Calls

- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [single_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/single_rop_buffer.md)
- [read_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [plan_mapi_store_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [empty_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/empty_session.md)