---
type: Rust Function
title: is_empty_synthetic_inbox_associated_config
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L458-L478
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_defaults
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
---

# Signature

`pub(super) fn is_empty_synthetic_inbox_associated_config( config: &MapiAssociatedConfigRecord, ) -> bool`

# Calls

- [outlook_inbox_associated_config_defaults](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_defaults.md)

# Called by

- [load_mapi_mail_store](../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [with_associated_configs](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)