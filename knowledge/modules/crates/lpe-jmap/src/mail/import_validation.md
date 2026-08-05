---
type: Rust Module
title: import_validation
resource: crates/lpe-jmap/src/mail/import_validation.rs#L1-L35
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/lpe-magika-ingresscontext-policydecision-validationrequest
  - external/crate-upload-expected-attachment-kind-jmapservice
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [validate_imported_attachments](../../../../../functions/crates/lpe-jmap/src/mail/import_validation/JmapService/validate_imported_attachments.md)

# Imports

- `anyhow::{bail, Result}`
- `lpe_magika::{IngressContext, PolicyDecision, ValidationRequest}`
- `crate::{upload::expected_attachment_kind, JmapService}`

# Member of

- [lpe-jmap](../../../../../packages/crates/lpe-jmap.md)