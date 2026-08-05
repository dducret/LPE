---
type: Rust Method
title: get_inbox_rules
resource: crates/lpe-exchange/src/service/ews/rules.rs#L8-L14
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/rules/get_inbox_rules_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_inbox_rules( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [get_inbox_rules_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/rules/get_inbox_rules_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)