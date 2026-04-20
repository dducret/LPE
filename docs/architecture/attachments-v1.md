# v1 Attachment Indexing

### Scope

`LPE` v1 indexes extracted text from:

- `PDF`
- `DOCX`
- `ODT`

Other office formats remain out of scope for v1.

### Selected libraries

- `pdf_oxide` for `PDF`
- `docx-lite` for `DOCX`
- internal `ODT` extractor based on ZIP and XML

### Reasons

- respects the `Apache-2.0` and `MIT` license constraint
- server-side local extraction
- indexing in `PostgreSQL`
- reusable later for search and local RAG

### Strategy

1. validate the incoming file type with Google `Magika` from the actual bytes
2. compare the detected type with the declared MIME type and extension
3. apply the acceptance, restriction, quarantine, or rejection policy
4. detect the attachment format for the retained file
5. extract raw text
6. normalize the text
7. store text in `attachments.extracted_text`
8. populate `attachments.extracted_text_tsv`
9. aggregate message and attachment search afterward

### Current v1 implementation

The first functional implementation covers:

- inbound delivery from `LPE-CT` when `LPE` receives a full RFC822 message
- `JMAP Email/import` from an uploaded RFC822 blob
- real text extraction for `PDF`, `DOCX`, and `ODT`
- population of `attachments.extracted_text` and `attachments.extracted_text_tsv`
- aggregation of message search with extracted attachment text
- an initial domain-scoped blob deduplication layer through `attachment_blobs`
- bootstrap `PST` export reconstruction of attachments through deduplicated blobs

Deduplication is strictly limited to the domain derived from the account primary email address. Two distinct domains never share attachment blobs even when the binary content is identical.

Each message still keeps its own `attachments` rows for mailbox semantics, search, and export, while the binary payload bytes may be shared in `attachment_blobs`.

### Current v1 limitations

- attachments are indexed only on flows where `LPE` already receives the complete MIME message
- canonical drafts and outbound submission do not yet expose a full end-user attachment model
- formats outside `PDF`, `DOCX`, and `ODT` may be retained as blobs but are not indexed
- no `OCR` is attempted for scanned or image-only `PDF` content

`Magika` validation is mandatory for every attachment or file entering through an external connection or through a client before indexing, import, or specialized parsing.

### Out of scope for v1

- OCR for scanned PDFs
- `ODS`, `XLSX`, `PPTX`, `ODP`
- advanced semantic extraction


