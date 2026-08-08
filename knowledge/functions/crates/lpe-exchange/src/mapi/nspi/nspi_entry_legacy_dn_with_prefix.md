---
type: Rust Function
title: nspi_entry_legacy_dn_with_prefix
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1080-L1110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_cn_from_source
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_dn_from_cn
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn
---

# Signature

`pub(in crate::mapi) fn nspi_entry_legacy_dn_with_prefix( entry: &ExchangeAddressBookEntry, include_kind_prefix: bool, ) -> String`

# Calls

- [nspi_legacy_cn_from_source](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_cn_from_source.md)
- [nspi_legacy_dn_from_cn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_legacy_dn_from_cn.md)

# Called by

- [nspi_entry_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_legacy_dn.md)
- [nspi_entry_unprefixed_legacy_dn](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_entry_unprefixed_legacy_dn.md)