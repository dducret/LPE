---
type: Rust Function
title: map_existing_recipients
resource: crates/lpe-jmap/src/convert.rs#L208-L218
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/update_draft
---

# Signature

`pub(crate) fn map_existing_recipients( recipients: &[JmapEmailAddress], ) -> Vec<SubmittedRecipientInput>`

# Called by

- [update_draft](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/update_draft.md)