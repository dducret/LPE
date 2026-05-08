# Data Lifecycle and Compliance

## Current State/Functionality Overview

`LPE` keeps canonical mailbox and collaboration state in PostgreSQL and protects metadata that must not appear in user search or AI pipelines. Retention, deletion, and legal hold rules must preserve auditability and exportability.

## Implementation/Usage

- Mailbox size tiers are quota policy inputs.
- `PST` import and export must preserve canonical messages and attachments.
- Attachment blobs are deduplicated per domain.
- Export must reconstruct every message with its blobs.
- Retention and legal hold must apply before destructive deletion.
- `Bcc`:
  - is protected metadata
  - must not be indexed in user search
  - must not be exposed to user-facing AI pipelines
  - must not be reinjected into visible projections
- Deletion must preserve required audit events.
- Attachment text extraction runs asynchronously.
- Indexed attachment formats are limited to `PDF`, `DOCX`, and `ODT`.

## Reference Table/List

| Data | Rule |
| --- | --- |
| primary store | `PostgreSQL` |
| search default | `PostgreSQL` |
| attachment dedupe | per domain |
| protected recipient metadata | `Bcc` |
| attachment text index formats | `PDF`, `DOCX`, `ODT` |
