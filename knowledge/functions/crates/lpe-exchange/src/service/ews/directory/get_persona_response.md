---
type: Rust Function
title: get_persona_response
resource: crates/lpe-exchange/src/service/ews/directory.rs#L178-L221
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/directory/requested_persona_id
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/directory/parse_persona_id
  - functions/crates/lpe-exchange/src/service/ews/directory/visible_persona_entries
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_persona
---

# Signature

`pub(in crate::service) fn get_persona_response( principal: &AccountPrincipal, request: &str, entries: &[ExchangeAddressBookEntry], ) -> String`

# Calls

- [requested_persona_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/requested_persona_id.md)
- [operation_error_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [parse_persona_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/parse_persona_id.md)
- [visible_persona_entries](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/visible_persona_entries.md)

# Called by

- [get_persona](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/ExchangeService/get_persona.md)