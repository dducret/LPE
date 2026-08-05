---
type: Rust Module
title: pst
resource: crates/lpe-storage/src/pst.rs#L1-L912
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-context-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-magika-read-validation-record-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/serde-serialize
  - external/sqlx-fromrow-postgres-row
  - external/std-fs-self-file
  - external/std-io-bufread-bufreader-write
  - external/std-path-path
  - external/uuid-uuid
  - external/crate-blob-store-durableblobkind-postgresblobstore-normalize-email-attachmentuploadinput-storage
  - external/super
  - external/crate-sha256-hex
  - external/sqlx-postgres-pgpooloptions
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [PstTransferJobRecord](../../../../classes/crates/lpe-storage/src/pst/PstTransferJobRecord.md)
- [NewPstTransferJob](../../../../classes/crates/lpe-storage/src/pst/NewPstTransferJob.md)
- [PstJobExecutionSummary](../../../../classes/crates/lpe-storage/src/pst/PstJobExecutionSummary.md)
- [PstTransferJobRow](../../../../classes/crates/lpe-storage/src/pst/PstTransferJobRow.md)
- [PendingPstJobRow](../../../../classes/crates/lpe-storage/src/pst/PendingPstJobRow.md)
- [PstImportedMessage](../../../../classes/crates/lpe-storage/src/pst/PstImportedMessage.md)
- [process_pending_pst_jobs](../../../../functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs.md)
- [mark_pst_job_running](../../../../functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_running.md)
- [mark_pst_job_completed](../../../../functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_completed.md)
- [mark_pst_job_failed](../../../../functions/crates/lpe-storage/src/pst/Storage/mark_pst_job_failed.md)
- [export_mailbox_to_pst](../../../../functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst.md)
- [import_mailbox_from_pst](../../../../functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst.md)
- [persist_pst_imported_message_in_tx](../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)
- [ensure_parent_directory](../../../../functions/crates/lpe-storage/src/pst/ensure_parent_directory.md)
- [validate_pst_import_path](../../../../functions/crates/lpe-storage/src/pst/validate_pst_import_path.md)
- [encode_pst_field](../../../../functions/crates/lpe-storage/src/pst/encode_pst_field.md)
- [decode_pst_field](../../../../functions/crates/lpe-storage/src/pst/decode_pst_field.md)
- [test_storage](../../../../functions/crates/lpe-storage/src/pst/test_storage.md)
- [insert_account_mailbox](../../../../functions/crates/lpe-storage/src/pst/insert_account_mailbox.md)
- [insert_message_with_attachment](../../../../functions/crates/lpe-storage/src/pst/insert_message_with_attachment.md)
- [insert_secondary_storage_pool](../../../../functions/crates/lpe-storage/src/pst/insert_secondary_storage_pool.md)
- [migrate_attachment_and_cleanup_source](../../../../functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source.md)
- [pst_export_reconstructs_attachment_after_old_placement_cleanup](../../../../functions/crates/lpe-storage/src/pst/pst_export_reconstructs_attachment_after_old_placement_cleanup.md)

# Imports

- `anyhow::{bail, Context, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_magika::{
    read_validation_record, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest,
    Validator,
}`
- `serde::Serialize`
- `sqlx::{FromRow, Postgres, Row}`
- `std::fs::{self, File}`
- `std::io::{BufRead, BufReader, Write}`
- `std::path::Path`
- `uuid::Uuid`
- `crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore},
    normalize_email, AttachmentUploadInput, Storage,
}`
- `super::*`
- `crate::sha256_hex`
- `sqlx::postgres::PgPoolOptions`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)