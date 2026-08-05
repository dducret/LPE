---
type: Rust Function
title: nspi_entry_unprefixed_legacy_dn
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1073-L1075
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn_with_prefix
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_dn_to_mid_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_search_key
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_entry_required_address_book_properties_match_exchange_identity_contract
  - functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address
---

# Signature

`pub(in crate::mapi) fn nspi_entry_unprefixed_legacy_dn(entry: &ExchangeAddressBookEntry) -> String`

# Calls

- [nspi_entry_legacy_dn_with_prefix](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn_with_prefix.md)

# Called by

- [principal_mailbox_store_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id.md)
- [nspi_match_dn_to_mid_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_match_dn_to_mid_entry.md)
- [nspi_entry_permanent_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_permanent_entry_id.md)
- [nspi_entry_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_search_key.md)
- [nspi_entry_match_rank](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_match_rank.md)
- [nspi_entry_value_with_directory](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_entry_value_with_directory.md)
- [nspi_entry_required_address_book_properties_match_exchange_identity_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_entry_required_address_book_properties_match_exchange_identity_contract.md)
- [mailbox_owner_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/mailbox_owner_entry_id.md)
- [legacy_dn_recipient_address](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address.md)