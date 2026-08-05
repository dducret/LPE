---
type: Rust Function
title: log_outlook_inbox_associated_config_bootstrap
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L425-L456
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(super) fn log_outlook_inbox_associated_config_bootstrap( account_id: Uuid, persisted: &[MapiAssociatedConfigRecord], inserted: &[MapiAssociatedConfigRecord], required_defaults: &[UpsertMapiAssociatedConfigInput], )`

# Called by

- [load_mapi_mail_store](../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)