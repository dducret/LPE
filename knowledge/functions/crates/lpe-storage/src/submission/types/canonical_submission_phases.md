---
type: Rust Function
title: canonical_submission_phases
resource: crates/lpe-storage/src/submission/types.rs#L89-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(super) fn canonical_submission_phases(has_source_draft: bool) -> Vec<CanonicalSubmissionPhase>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)