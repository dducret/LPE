---
type: Rust Function
title: persona_id
resource: crates/lpe-exchange/src/service/ews/directory.rs#L392-L398
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/visible_persona_entries
  - functions/crates/lpe-exchange/src/service/ews/directory/persona_xml
---

# Signature

`fn persona_id(entry: &ExchangeAddressBookEntry) -> String`

# Called by

- [visible_persona_entries](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/visible_persona_entries.md)
- [persona_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/persona_xml.md)