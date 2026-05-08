# Local AI

## Current State/Functionality Overview

AI features must support local-only execution and must not require data to leave the server. AI-facing pipelines must exclude protected metadata such as `Bcc`.

## Implementation/Usage

- Keep model execution replaceable and local-compatible.
- Store provenance for generated artifacts.
- Use canonical message, contact, calendar, task, and attachment projections.
- Validate files with Google `Magika` before processing.
- Limit attachment text indexing to `PDF`, `DOCX`, and `ODT`.
- Exclude `Bcc` from:
  - search indexes
  - prompt context
  - embeddings
  - summaries
  - user-facing AI outputs
- Do not assume cloud model access.
- Do not add an external AI dependency without license review under `LICENSE.md`.

## Reference Table/List

| Area | Rule |
| --- | --- |
| execution | local-compatible |
| protected metadata | no `Bcc` |
| attachment formats | `PDF`, `DOCX`, `ODT` |
| provenance | required for AI artifacts |
| dependency review | required |
