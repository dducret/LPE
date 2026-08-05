---
type: Rust Function
title: visible_persona_entries
resource: crates/lpe-exchange/src/service/ews/directory.rs#L335-L369
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/directory/persona_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/find_people_response
  - functions/crates/lpe-exchange/src/service/ews/directory/get_persona_response
---

# Signature

`fn visible_persona_entries( principal: &AccountPrincipal, entries: &[ExchangeAddressBookEntry], ) -> Vec<ExchangeAddressBookEntry>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [persona_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/persona_id.md)

# Called by

- [find_people_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/find_people_response.md)
- [get_persona_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/get_persona_response.md)