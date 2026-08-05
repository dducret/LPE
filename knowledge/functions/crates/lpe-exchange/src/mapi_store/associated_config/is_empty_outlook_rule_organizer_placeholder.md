---
type: Rust Function
title: is_empty_outlook_rule_organizer_placeholder
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L480-L491
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(super) fn is_empty_outlook_rule_organizer_placeholder( config: &MapiAssociatedConfigRecord, ) -> bool`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [load_mapi_mail_store](../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)