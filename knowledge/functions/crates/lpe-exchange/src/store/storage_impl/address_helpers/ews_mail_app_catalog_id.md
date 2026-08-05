---
type: Rust Function
title: ews_mail_app_catalog_id
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L1300-L1324
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/ews_update_mail_app_install_status
---

# Signature

`async fn ews_mail_app_catalog_id( storage: &Storage, principal: &AccountPrincipal, app_id: &str, ) -> Result<Uuid>`

# Calls

- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)

# Called by

- [ews_update_mail_app_install_status](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/ews_update_mail_app_install_status.md)