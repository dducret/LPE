---
type: Rust Function
title: reject_unknown_email_properties
resource: crates/lpe-jmap/src/drafts.rs#L97-L106
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/drafts/parse_draft_mutation
---

# Signature

`pub(crate) fn reject_unknown_email_properties(object: &Map<String, Value>) -> Result<()>`

# Called by

- [parse_draft_mutation](../../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_mutation.md)