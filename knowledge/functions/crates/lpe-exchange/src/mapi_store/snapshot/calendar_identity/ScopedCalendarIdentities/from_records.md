---
type: Rust Method
title: from_records
resource: crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity.rs#L15-L46
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/default_calendar_uses_reserved_fid_without_an_identity_record
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/custom_calendar_fails_closed_without_a_principal_scoped_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities
---

# Signature

`fn from_records( records: &[MapiIdentityRecord], identity_codec: &crate::mapi::identity::MapiIdentityCodec, ) -> Self`

# Called by

- [default_calendar_uses_reserved_fid_without_an_identity_record](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/default_calendar_uses_reserved_fid_without_an_identity_record.md)
- [custom_calendar_fails_closed_without_a_principal_scoped_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/custom_calendar_fails_closed_without_a_principal_scoped_identity.md)
- [new_with_scoped_calendar_identities](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities.md)