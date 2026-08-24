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
- Mark only the first eligible non-attachment iMIP `text/calendar` part as
  `mime_parts.is_scheduling_body`. Apply the `[MS-OXCMAIL]` selection rules
  inside `multipart/alternative` and `multipart/related`; for Outlook 2010 and
  later compatibility, `[MS-STANOICAL]` V0334 requires searching every
  `multipart/mixed` child in order. Later calendar candidates and every part
  with `Content-Disposition: attachment` remain attachments. Keep the selected
  part in canonical MIME/blob state for
  reconstruction, but correlate and omit only its exact MIME-part identity from
  the actionable meeting request or response MAPI attachment projection.
  An explicitly attached calendar file remains a visible attachment.
- For outbound actionable scheduling mail, emit the selected calendar body in
  the `[MS-OXCMAIL]` sections 2.1.3.3.8.2 and 2.1.3.3.8.3
  `multipart/alternative` shape. When other attachments exist, make that body
  the first child of `multipart/mixed` and serialize every non-selected part,
  including explicitly attached calendar files, as an ordinary MIME attachment.
  A selected scheduling body must parse completely as `REQUEST`, `REPLY`, or
  `COUNTER`; reject malformed bodies and unsupported methods before canonical
  submission writes.
- For ordinary outbound mail with attachments, emit the text body first in
  `multipart/mixed` and serialize every attachment into the transport MIME;
  canonical Sent storage without matching wire MIME is not sufficient.
- When a saved Draft or Outbox message is hydrated for editing or submission,
  take each attachment media type and scheduling marker from its canonical MIME
  part, not from deduplicated blob metadata. Read its durable bytes through the
  active source transaction so a one-connection pool cannot deadlock waiting
  for a second checkout.
- Persist the parser revision, durable classification generation,
  classification, exact scheduling MIME-part ID, dirty-reclassification state,
  and bounded request/response metadata in `calendar_mail_classifications`.
  Preserve that row and clear its MIME-part reference before replacing or
  deleting the selected part. Calendar metadata reads must hydrate only that
  selected part through the durable blob backend, including migrated/S3 blobs,
  rather than reading PostgreSQL blob bytes directly, then revalidate the exact
  part identity under the message lock before committing the result.
- Persist the SHA-256 of a selected Meeting Response body on the canonical
  message only after the inbound attendee, SMTP envelope sender, unambiguous
  RFC `From`, and supplied organizer all match the recipient account. Scheduling
  content uses the exact base media type `text/calendar`; lookalike types are
  never scheduling bodies. Lazy classification may restore `REPLY` or `COUNTER`
  metadata only when the current selected part has that exact authorized hash.
  After locking the message, recheck the blob identity and hash, exact media
  type, attachment identity, authorization hash, and processed state before
  committing repaired metadata. Keep the authoritative server-processed bit
  with the same message, override stale classification JSON with it on clean
  reads, and project it true only for applied, idempotent, or superseded
  responses so Outlook cannot apply an already-handled response again.
- Track each visible account's applied generation in
  `calendar_mail_classification_projections`. An actionable metadata transition,
  including actionable to `none`, must rotate and journal every visible
  account's message projection before that account acknowledges the generation;
  original inbound memberships and an account's first visible copied membership
  acknowledge the generation they were created with. Later mailbox copies must
  preserve any generation pending on an older membership, and initial copy
  acknowledgement must be insert-only so retained state from an expunged
  membership is not overwritten. Return calendar metadata only from a single
  snapshot in which the classification is clean and every visible account has
  applied the current generation.
- Keep `Bcc` out of user search and AI-facing pipelines.
- Use asynchronous extraction for attachment text.
- Storage module split plan: `crates/lpe-storage/src/attachments.rs` currently owns both
  attachment mutation transactions and message/calendar blob-content projections. Before the
  next attachment behavior is added, move the read projections into an `attachments/content.rs`
  sibling while retaining canonical create/delete transactions in the current module. This P0
  work changes only those existing paths and does not add another attachment abstraction.
- Do not extend indexed file types without updating architecture documentation.
- The authenticated web client uploads one attachment to an existing canonical draft through
  `POST /api/mail/messages/{messageId}/attachments?accountId={mailboxAccountId}` as multipart
  field `file`. The target mailbox must be owned by the principal or have canonical delegated
  write access; the upload is validated with Magika before it is attached to the draft.
- The authenticated web client retrieves message attachments through
  `GET /api/mail/messages/{messageId}/attachments/{attachmentId}?accountId={mailboxAccountId}`.
  The target mailbox must be owned by the principal or be canonically readable, and the response
  is read from the existing durable attachment blob path.

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
