---
type: Rust Function
title: parse_draft_keywords
resource: crates/lpe-jmap/src/drafts.rs#L47-L70
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  called_by:
  - functions/crates/lpe-jmap/src/drafts/parse_draft_mutation
---

# Signature

`fn parse_draft_keywords(value: Option<&Value>) -> Result<ParsedDraftKeywords>`

# Calls

- [as_bool](../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)

# Called by

- [parse_draft_mutation](../../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_mutation.md)