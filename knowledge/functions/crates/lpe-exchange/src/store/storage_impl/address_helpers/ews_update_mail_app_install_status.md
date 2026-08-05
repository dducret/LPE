---
type: Rust Function
title: ews_update_mail_app_install_status
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L1249-L1285
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/ews_mail_app_catalog_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn ews_update_mail_app_install_status( storage: &Storage, principal: &AccountPrincipal, app_id: &str, status: &str, audit: AuditEntryInput, ) -> Result<EwsMailAppInstall>`

# Calls

- [ews_mail_app_catalog_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/ews_mail_app_catalog_id.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)