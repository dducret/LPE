---
type: Rust Module
title: convert
resource: crates/lpe-jmap/src/convert.rs#L1-L230
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/serde-serialize
  - external/serde-json-json-map-value
  - external/std-collections-hashmap
  - external/crate-protocol-emailaddressinput
  - external/lpe-storage-mail-parsedmailaddress-authenticatedaccount-jmapemailaddress-mailboxaccountaccess-submittedrecipientinput
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [format_addresses](../../../../functions/crates/lpe-jmap/src/convert/format_addresses.md)
- [insert_if](../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)
- [has_jmap_property_patch](../../../../functions/crates/lpe-jmap/src/convert/has_jmap_property_patch.md)
- [apply_jmap_property_patch](../../../../functions/crates/lpe-jmap/src/convert/apply_jmap_property_patch.md)
- [apply_jmap_property_path](../../../../functions/crates/lpe-jmap/src/convert/apply_jmap_property_path.md)
- [unescape_property_path_segment](../../../../functions/crates/lpe-jmap/src/convert/unescape_property_path_segment.md)
- [address_value](../../../../functions/crates/lpe-jmap/src/convert/address_value.md)
- [resolve_creation_reference](../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)
- [select_from_addresses](../../../../functions/crates/lpe-jmap/src/convert/select_from_addresses.md)
- [map_recipients](../../../../functions/crates/lpe-jmap/src/convert/map_recipients.md)
- [map_existing_recipients](../../../../functions/crates/lpe-jmap/src/convert/map_existing_recipients.md)
- [map_parsed_recipients](../../../../functions/crates/lpe-jmap/src/convert/map_parsed_recipients.md)

# Imports

- `anyhow::{bail, Result}`
- `serde::Serialize`
- `serde_json::{json, Map, Value}`
- `std::collections::HashMap`
- `crate::protocol::EmailAddressInput`
- `lpe_storage::{
    mail::ParsedMailAddress, AuthenticatedAccount, JmapEmailAddress, MailboxAccountAccess,
    SubmittedRecipientInput,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)