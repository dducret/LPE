---
type: Rust Function
title: associated_config_uuid
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L614-L630
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
---

# Signature

`pub(super) fn associated_config_uuid(properties: &HashMap<u32, MapiValue>) -> Uuid`

# Calls

- [imported_message_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)