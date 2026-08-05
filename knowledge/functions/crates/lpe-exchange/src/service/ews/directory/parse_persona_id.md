---
type: Rust Function
title: parse_persona_id
resource: crates/lpe-exchange/src/service/ews/directory.rs#L400-L413
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/get_persona_response
---

# Signature

`fn parse_persona_id(value: &str) -> Option<(ExchangeAddressBookEntryKind, Uuid)>`

# Called by

- [get_persona_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/get_persona_response.md)