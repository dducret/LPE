---
type: Rust Function
title: remember_nspi_identity_records
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L578-L603
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_identity_kind_key_for_request
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/remember_nspi_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity
---

# Signature

`async fn remember_nspi_identity_records<S>( store: &S, principal: &AccountPrincipal, requests: &[MapiIdentityRequest], ) -> Result<()> where S: ExchangeStore,`

# Calls

- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [nspi_identity_kind_key_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_identity_kind_key_for_request.md)
- [remember_nspi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/remember_nspi_identity.md)

# Called by

- [allocate_nspi_entry_identities](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_nspi_entry_identities.md)
- [allocate_principal_nspi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/allocate_principal_nspi_identity.md)