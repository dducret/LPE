---
type: Rust Function
title: get_app_manifests_response
resource: crates/lpe-exchange/src/service/ews/mail_apps.rs#L209-L251
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_app_manifests
---

# Signature

`pub(in crate::service) fn get_app_manifests_response(manifests: &[EwsMailAppManifest]) -> String`

# Called by

- [get_app_manifests](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail_apps/ExchangeService/get_app_manifests.md)