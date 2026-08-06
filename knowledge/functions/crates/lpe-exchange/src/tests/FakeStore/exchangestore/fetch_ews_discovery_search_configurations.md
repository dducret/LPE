---
type: Rust Method
title: fetch_ews_discovery_search_configurations
resource: crates/lpe-exchange/src/tests/mod.rs#L5266-L5272
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_discovery_search_configuration
---

# Signature

`fn fetch_ews_discovery_search_configurations<'a>( &'a self, _principal: &'a AccountPrincipal, ) -> StoreFuture<'a, Vec<EwsDiscoverySearchConfig>>`

# Called by

- [get_discovery_search_configuration](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_discovery_search_configuration.md)