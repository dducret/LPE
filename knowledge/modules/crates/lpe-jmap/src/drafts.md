---
type: Rust Module
title: drafts
resource: crates/lpe-jmap/src/drafts.rs#L1-L106
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/serde-json-map-value
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/crate-parse-parse-address-list-parse-optional-nullable-string-parse-optional-string-parse-uuid-protocol-draftmutation-resolve-creation-reference
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [parse_draft_mutation](../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_mutation.md)
- [ParsedDraftKeywords](../../../../classes/crates/lpe-jmap/src/drafts/ParsedDraftKeywords.md)
- [parse_draft_keywords](../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_keywords.md)
- [parse_email_copy](../../../../functions/crates/lpe-jmap/src/drafts/parse_email_copy.md)
- [reject_unknown_email_properties](../../../../functions/crates/lpe-jmap/src/drafts/reject_unknown_email_properties.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `serde_json::{Map, Value}`
- `std::collections::HashMap`
- `uuid::Uuid`
- `crate::{
    parse::{
        parse_address_list, parse_optional_nullable_string, parse_optional_string, parse_uuid,
    },
    protocol::DraftMutation,
    resolve_creation_reference,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)