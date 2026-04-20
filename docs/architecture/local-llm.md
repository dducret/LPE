# LLM local-first

### Goal

`LPE` must be able to integrate a local LLM later, for example a Gemma-like family, without sending data outside the server and without making AI mandatory for the product core.

### Principles

- all user data stays on `LPE` infrastructure
- the AI engine reads normalized document projections, not raw MIME as the primary source
- all AI requests are ACL-filtered before inference
- protected metadata such as `Bcc` is excluded from user-facing projections, chunks, and inference inputs; it remains stored separately from visible recipients and outside default search indexes
- every AI result keeps usable provenance
- PostgreSQL search remains the primary layer for precision, speed, and filtering

### Data strategy

Each important business object must be able to produce a canonical projection:

- message
- calendar event
- contact
- attachment

Each projection must provide:

- stable identifier
- source type
- normalized text
- language
- normalized participants
- content fingerprint
- owner and ACL fingerprint

### Pipeline

1. ingest the message or business object
2. normalize content
3. index with PostgreSQL full-text search
4. optionally create chunks
5. run asynchronous local AI enrichments
6. store AI artifacts with provenance

### Planned artifacts

- `document_projections`
- `document_chunks`
- `document_annotations`
- `inference_runs`
- `inference_run_chunks`

### Model interface

The backend exposes a generic local provider contract:

- describe available local models
- execute an inference
- return output and provenance for the chunks used

The product core must not depend on a single model. `Gemma` should be one future local provider among others.



