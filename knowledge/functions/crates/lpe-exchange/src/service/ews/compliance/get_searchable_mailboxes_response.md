---
type: Rust Function
title: get_searchable_mailboxes_response
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L150-L189
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_searchable_mailboxes
---

# Signature

`pub(in crate::service) fn get_searchable_mailboxes_response( mailboxes: &[EwsSearchableMailbox], ) -> String`

# Called by

- [get_searchable_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_searchable_mailboxes.md)