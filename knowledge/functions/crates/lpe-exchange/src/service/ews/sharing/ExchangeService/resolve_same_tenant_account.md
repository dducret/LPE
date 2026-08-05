---
type: Rust Method
title: resolve_same_tenant_account
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L166-L180
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_folder
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation
---

# Signature

`async fn resolve_same_tenant_account( &self, principal: &AccountPrincipal, email: &str, ) -> Result<ExchangeAddressBookEntry>`

# Calls

- [fetch_address_book_entries](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_address_book_entries.md)

# Called by

- [get_sharing_folder](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_folder.md)
- [accept_sharing_invitation](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation.md)