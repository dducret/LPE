---
type: Rust Function
title: parse_draft_mutation
resource: crates/lpe-jmap/src/drafts.rs#L14-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/drafts/reject_unknown_email_properties
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/drafts/parse_draft_keywords
  - functions/crates/lpe-jmap/src/parse/parse_optional_string
  - functions/crates/lpe-jmap/src/parse/parse_optional_nullable_string
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/create_draft
  - functions/crates/lpe-jmap/src/mail/JmapService/update_draft
---

# Signature

`pub(crate) fn parse_draft_mutation(value: Value) -> Result<DraftMutation>`

# Calls

- [reject_unknown_email_properties](../../../../../functions/crates/lpe-jmap/src/drafts/reject_unknown_email_properties.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_draft_keywords](../../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_keywords.md)
- [parse_optional_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_optional_string.md)
- [parse_optional_nullable_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_optional_nullable_string.md)

# Called by

- [create_draft](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/create_draft.md)
- [update_draft](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/update_draft.md)