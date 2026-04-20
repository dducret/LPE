# Data Lifecycle and Compliance

### Goal

This document covers mailbox storage, archiving, retention, deduplication, privacy, and asynchronous processing.

### Mailbox size tiers

- below `10 GB`: shared database
- above `10 GB`: dedicated database
- above `50 GB`: user-invisible technical partitioning
- above `100 GB`: online archive with degraded performance

### `PST` import and export

`LPE` supports full or partial `PST` import and export.

### Deduplication

Identical blobs are deduplicated at domain scope.

Export paths must reconstruct attachments correctly.

### Retention and legal hold

Retention may be defined at:

- tenant
- domain
- mailbox
- folder

Legal hold is governed at tenant level.

### `Bcc`

`Bcc` is protected metadata.

It:

- is not indexed in standard search
- is excluded from user-facing AI pipelines
- remains available for audit and compliance through privileged workflows

### Deletion and audit

Deleted messages must be reconstructed and moved into a separated `Audit Store` so they do not pollute production indexes.

### Asynchronous text extraction

Attachment text extraction must not happen in the synchronous `SMTP` receive path.


