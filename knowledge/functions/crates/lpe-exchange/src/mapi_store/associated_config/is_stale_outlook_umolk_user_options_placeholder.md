---
type: Rust Function
title: is_stale_outlook_umolk_user_options_placeholder
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L493-L514
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
---

# Signature

`pub(super) fn is_stale_outlook_umolk_user_options_placeholder( config: &MapiAssociatedConfigRecord, ) -> bool`

# Calls

- [is_outlook_umolk_user_options_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [load_mapi_mail_store](../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [with_associated_configs](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)