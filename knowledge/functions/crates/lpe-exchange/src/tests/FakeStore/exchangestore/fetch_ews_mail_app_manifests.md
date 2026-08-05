---
type: Rust Method
title: fetch_ews_mail_app_manifests
resource: crates/lpe-exchange/src/tests/mod.rs#L5529-L5556
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_app_manifests
---

# Signature

`fn fetch_ews_mail_app_manifests<'a>( &'a self, principal: &'a AccountPrincipal, ) -> StoreFuture<'a, Vec<EwsMailAppManifest>>`

# Called by

- [get_app_manifests](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_app_manifests.md)