---
type: Rust Method
title: fetch_ews_app_marketplace_policy
resource: crates/lpe-exchange/src/tests/mod.rs#L5558-L5564
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_app_marketplace_url
---

# Signature

`fn fetch_ews_app_marketplace_policy<'a>( &'a self, _principal: &'a AccountPrincipal, ) -> StoreFuture<'a, EwsAppMarketplacePolicy>`

# Called by

- [get_app_marketplace_url](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_app_marketplace_url.md)