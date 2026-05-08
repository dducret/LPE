# Attachment Indexing

## Current State/Functionality Overview

Attachments are validated before processing and indexed only for the documented text formats. Identical blobs are deduplicated per domain while export reconstructs each message with its blobs.

## Implementation/Usage

- Validate every external or client-provided file with Google `Magika` before normal processing.
- Index text only for:
  - `PDF`
  - `DOCX`
  - `ODT`
- Store attachment metadata separately from deduplicated blobs.
- Deduplicate identical attachments per domain through `attachment_blobs`.
- Keep export able to reconstruct messages with the original blobs.
- Keep `Bcc` out of user search and AI-facing pipelines.
- Use asynchronous extraction for attachment text.
- Do not extend indexed file types without updating architecture documentation.

## Reference Table/List

| Format | Status |
| --- | --- |
| `PDF` | indexed |
| `DOCX` | indexed |
| `ODT` | indexed |
| other formats | validated, not text-indexed |

| Library/Tool | Purpose | License note |
| --- | --- | --- |
| Google `Magika` | file-type validation | acceptable Apache-2.0 candidate; integration dependencies require review |
| `docx-lite` | DOCX extraction | accepted `MIT` exception in `LICENSE.md` |
