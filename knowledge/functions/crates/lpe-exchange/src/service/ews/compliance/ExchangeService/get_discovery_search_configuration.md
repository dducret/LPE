---
type: Rust Method
title: get_discovery_search_configuration
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L8-L17
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_discovery_search_configurations
  - functions/crates/lpe-exchange/src/service/ews/compliance/get_discovery_search_configuration_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_discovery_search_configuration( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_ews_discovery_search_configurations](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_discovery_search_configurations.md)
- [get_discovery_search_configuration_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/get_discovery_search_configuration_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)