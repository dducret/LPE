---
type: Rust Module
title: service
resource: crates/lpe-core/src/service.rs#L1-L62
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-attachments-attachmentformat
  - external/lpe-domain-accessscope-account-documentchunk-documentkind-documentprojection
  - external/std-env
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-core
---

# Contains

- [CoreService](../../../../classes/crates/lpe-core/src/service/CoreService.md)
- [bootstrap_admin_account](../../../../functions/crates/lpe-core/src/service/CoreService/bootstrap_admin_account.md)
- [bootstrap_mail_projection](../../../../functions/crates/lpe-core/src/service/CoreService/bootstrap_mail_projection.md)
- [bootstrap_projection_chunks](../../../../functions/crates/lpe-core/src/service/CoreService/bootstrap_projection_chunks.md)
- [supported_attachment_formats](../../../../functions/crates/lpe-core/src/service/CoreService/supported_attachment_formats.md)

# Imports

- `anyhow::Result`
- `lpe_attachments::AttachmentFormat`
- `lpe_domain::{AccessScope, Account, DocumentChunk, DocumentKind, DocumentProjection}`
- `std::env`
- `uuid::Uuid`

# Member of

- [lpe-core](../../../../packages/crates/lpe-core.md)