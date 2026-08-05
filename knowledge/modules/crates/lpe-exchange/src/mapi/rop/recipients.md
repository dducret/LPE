---
type: Rust Module
title: recipients
resource: crates/lpe-exchange/src/mapi/rop/recipients.rs#L1-L313
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-format-property-tags-for-debug-parse-property-value-for-tag-cursor-roprequest
  - external/crate-mapi-nspi-normalize-nspi-lookup-value-nspi-entry-legacy-dn-nspi-entry-unprefixed-legacy-dn-principal-address-book-entry-properties-canonical-property-storage-tag-normalize-mapi-submit-address-mapivalue-pid-tag-address-book-display-name-printable-w-pid-tag-display-name-w-pid-tag-email-address-w-pid-tag-recipient-display-name-w-pid-tag-recipient-type-pid-tag-smtp-address-w-session-pendingrecipient-session-pendingrecipientchange-store-exchangeaddressbookentry
  - external/anyhow-anyhow-result
  - external/lpe-mail-auth-accountprincipal
  - external/std-collections-hashmap
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [modify_recipients](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients.md)
- [parse_pending_recipient_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row.md)
- [parse_simple_pending_recipient_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_simple_pending_recipient_row.md)
- [parse_wrapped_pending_recipient_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)
- [recipient_display_name_from_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/recipient_display_name_from_values.md)
- [optional_mapi_value_text](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/optional_mapi_value_text.md)
- [normalize_recipient_type](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/normalize_recipient_type.md)
- [legacy_dn_recipient_address](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_recipient_address.md)
- [legacy_dn_matches_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/legacy_dn_matches_entry.md)
- [read_recipient_string](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/read_recipient_string.md)

# Imports

- `super::{format_property_tags_for_debug, parse_property_value_for_tag, Cursor, RopRequest}`
- `crate::{
    mapi::{
        nspi::{
            normalize_nspi_lookup_value, nspi_entry_legacy_dn, nspi_entry_unprefixed_legacy_dn,
            principal_address_book_entry,
        },
        properties::{
            canonical_property_storage_tag, normalize_mapi_submit_address, MapiValue,
            PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W, PID_TAG_DISPLAY_NAME_W,
            PID_TAG_EMAIL_ADDRESS_W, PID_TAG_RECIPIENT_DISPLAY_NAME_W, PID_TAG_RECIPIENT_TYPE,
            PID_TAG_SMTP_ADDRESS_W,
        },
        session::PendingRecipient,
        session::PendingRecipientChange,
    },
    store::ExchangeAddressBookEntry,
}`
- `anyhow::{anyhow, Result}`
- `lpe_mail_auth::AccountPrincipal`
- `std::collections::HashMap`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)