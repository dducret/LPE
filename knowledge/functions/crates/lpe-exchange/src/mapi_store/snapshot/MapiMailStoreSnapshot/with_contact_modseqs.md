---
type: Rust Method
title: with_contact_modseqs
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L126-L135
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn with_contact_modseqs(mut self, versions: Vec<(Uuid, String)>) -> Self`

# Called by

- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)