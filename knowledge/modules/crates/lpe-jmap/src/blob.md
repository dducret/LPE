---
type: Rust Module
title: blob
resource: crates/lpe-jmap/src/blob.rs#L1-L538
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-magika-expectedkind-ingresscontext-policydecision-validationrequest
  - external/serde-deserialize
  - external/serde-json-json-map-value
  - external/sha1-sha1
  - external/sha2-digest-as-sha256
  - external/std-collections-hashmap-hashset
  - external/lpe-storage-authenticatedaccount-jmapemail-mailboxaccountaccess
  - external/crate-blob-id-for-message-error-method-error-set-error-resolve-creation-reference-jmapservice-jmap-mail-capability-max-blob-data-sources-max-size-upload
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [BlobCopyArguments](../../../../classes/crates/lpe-jmap/src/blob/BlobCopyArguments.md)
- [BlobUploadArguments](../../../../classes/crates/lpe-jmap/src/blob/BlobUploadArguments.md)
- [BlobUploadObject](../../../../classes/crates/lpe-jmap/src/blob/BlobUploadObject.md)
- [BlobDataSource](../../../../classes/crates/lpe-jmap/src/blob/BlobDataSource.md)
- [BlobGetArguments](../../../../classes/crates/lpe-jmap/src/blob/BlobGetArguments.md)
- [BlobLookupArguments](../../../../classes/crates/lpe-jmap/src/blob/BlobLookupArguments.md)
- [handle_blob_upload](../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload.md)
- [handle_blob_get](../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get.md)
- [handle_blob_lookup](../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup.md)
- [handle_blob_copy](../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy.md)
- [build_blob_upload](../../../../functions/crates/lpe-jmap/src/blob/JmapService/build_blob_upload.md)
- [resolve_upload_source](../../../../functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source.md)
- [ensure_blob_create_allowed](../../../../functions/crates/lpe-jmap/src/blob/ensure_blob_create_allowed.md)
- [BlobLookupEntry](../../../../classes/crates/lpe-jmap/src/blob/BlobLookupEntry.md)
- [blob_lookup_index](../../../../functions/crates/lpe-jmap/src/blob/blob_lookup_index.md)
- [sorted_values](../../../../functions/crates/lpe-jmap/src/blob/sorted_values.md)
- [slice_blob_range](../../../../functions/crates/lpe-jmap/src/blob/slice_blob_range.md)
- [blob_get_object](../../../../functions/crates/lpe-jmap/src/blob/blob_get_object.md)
- [readable_blob_range](../../../../functions/crates/lpe-jmap/src/blob/readable_blob_range.md)
- [unsupported_blob_get_property](../../../../functions/crates/lpe-jmap/src/blob/unsupported_blob_get_property.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest}`
- `serde::Deserialize`
- `serde_json::{json, Map, Value}`
- `sha1::Sha1`
- `sha2::{Digest as _, Sha256}`
- `std::collections::{HashMap, HashSet}`
- `lpe_storage::{AuthenticatedAccount, JmapEmail, MailboxAccountAccess}`
- `crate::{
    blob_id_for_message,
    error::{method_error, set_error},
    resolve_creation_reference, JmapService, JMAP_MAIL_CAPABILITY, MAX_BLOB_DATA_SOURCES,
    MAX_SIZE_UPLOAD,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)