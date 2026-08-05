---
type: Rust Module
title: storage_backend
resource: crates/lpe-storage/src/storage_backend.rs#L1-L946
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-crypto-hex-lower-hmac-sha256-hmac-sha256-hex-sha256-hex
  - external/lpe-domain-utc-from-unix-seconds
  - external/reqwest-header-headermap-headername-headervalue-content-length-client-method-statuscode-url
  - external/serde-json-json-map-value
  - external/std-collections-btreemap-env-fmt-time-systemtime-unix-epoch
  - external/uuid-uuid
  - external/crate-storagepoolconfigsummary
  - external/super
  - external/serde-json-json
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [StorageBackendSelection](../../../../classes/crates/lpe-storage/src/storage_backend/StorageBackendSelection.md)
- [S3CompatiblePoolConfig](../../../../classes/crates/lpe-storage/src/storage_backend/S3CompatiblePoolConfig.md)
- [S3ObjectStat](../../../../classes/crates/lpe-storage/src/storage_backend/S3ObjectStat.md)
- [StorageBackendError](../../../../classes/crates/lpe-storage/src/storage_backend/StorageBackendError.md)
- [fmt](../../../../functions/crates/lpe-storage/src/storage_backend/StorageBackendError/fmt-display/fmt.md)
- [S3Credentials](../../../../classes/crates/lpe-storage/src/storage_backend/S3Credentials.md)
- [S3AddressingStyle](../../../../classes/crates/lpe-storage/src/storage_backend/S3AddressingStyle.md)
- [as_str](../../../../functions/crates/lpe-storage/src/storage_backend/S3AddressingStyle/as_str.md)
- [normalize_storage_pool_kind](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_kind.md)
- [normalize_storage_pool_config](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config.md)
- [select_storage_backend](../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)
- [storage_pool_config_summary](../../../../functions/crates/lpe-storage/src/storage_backend/storage_pool_config_summary.md)
- [s3_object_key_for_placement](../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement.md)
- [s3_put_object](../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [s3_read_object](../../../../functions/crates/lpe-storage/src/storage_backend/s3_read_object.md)
- [s3_stat_object](../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)
- [s3_probe_pool](../../../../functions/crates/lpe-storage/src/storage_backend/s3_probe_pool.md)
- [map_s3_status_error](../../../../functions/crates/lpe-storage/src/storage_backend/map_s3_status_error.md)
- [normalize_postgres_config](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_postgres_config.md)
- [normalize_s3_compatible_config](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_s3_compatible_config.md)
- [parse_s3_compatible_config](../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config.md)
- [parse_s3_compatible_config_from_map](../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)
- [reject_forbidden_or_unknown_fields](../../../../functions/crates/lpe-storage/src/storage_backend/reject_forbidden_or_unknown_fields.md)
- [required_string](../../../../functions/crates/lpe-storage/src/storage_backend/required_string.md)
- [optional_string](../../../../functions/crates/lpe-storage/src/storage_backend/optional_string.md)
- [required_region](../../../../functions/crates/lpe-storage/src/storage_backend/required_region.md)
- [normalize_endpoint_url](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_endpoint_url.md)
- [normalize_bucket](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_bucket.md)
- [normalize_signing_region](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_signing_region.md)
- [normalize_addressing_style](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_addressing_style.md)
- [normalize_object_prefix](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_object_prefix.md)
- [resolve_s3_credentials](../../../../functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials.md)
- [env_secret](../../../../functions/crates/lpe-storage/src/storage_backend/env_secret.md)
- [s3_object_url](../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_url.md)
- [s3_bucket_url](../../../../functions/crates/lpe-storage/src/storage_backend/s3_bucket_url.md)
- [signed_s3_request](../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)
- [stat_from_headers](../../../../functions/crates/lpe-storage/src/storage_backend/stat_from_headers.md)
- [ensure_success_status](../../../../functions/crates/lpe-storage/src/storage_backend/ensure_success_status.md)
- [map_reqwest_error](../../../../functions/crates/lpe-storage/src/storage_backend/map_reqwest_error.md)
- [s3_signing_key](../../../../functions/crates/lpe-storage/src/storage_backend/s3_signing_key.md)
- [canonical_host](../../../../functions/crates/lpe-storage/src/storage_backend/canonical_host.md)
- [canonical_uri](../../../../functions/crates/lpe-storage/src/storage_backend/canonical_uri.md)
- [normalize_header_value](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_header_value.md)
- [percent_encode_path](../../../../functions/crates/lpe-storage/src/storage_backend/percent_encode_path.md)
- [percent_encode_segment](../../../../functions/crates/lpe-storage/src/storage_backend/percent_encode_segment.md)
- [s3_timestamp](../../../../functions/crates/lpe-storage/src/storage_backend/s3_timestamp.md)
- [normalize_credentials_ref](../../../../functions/crates/lpe-storage/src/storage_backend/normalize_credentials_ref.md)
- [has_control_or_whitespace](../../../../functions/crates/lpe-storage/src/storage_backend/has_control_or_whitespace.md)
- [postgres_backend_accepts_empty_config_only](../../../../functions/crates/lpe-storage/src/storage_backend/postgres_backend_accepts_empty_config_only.md)
- [s3_compatible_backend_normalizes_provider_neutral_config](../../../../functions/crates/lpe-storage/src/storage_backend/s3_compatible_backend_normalizes_provider_neutral_config.md)
- [s3_compatible_backend_rejects_inline_credentials](../../../../functions/crates/lpe-storage/src/storage_backend/s3_compatible_backend_rejects_inline_credentials.md)
- [s3_compatible_summary_redacts_secret_reference](../../../../functions/crates/lpe-storage/src/storage_backend/s3_compatible_summary_redacts_secret_reference.md)
- [object_key_is_deterministic_and_omits_tenant_domain_material](../../../../functions/crates/lpe-storage/src/storage_backend/object_key_is_deterministic_and_omits_tenant_domain_material.md)
- [s3_status_errors_are_storage_backend_errors](../../../../functions/crates/lpe-storage/src/storage_backend/s3_status_errors_are_storage_backend_errors.md)
- [s3_signing_timestamp_uses_utc_amz_format](../../../../functions/crates/lpe-storage/src/storage_backend/s3_signing_timestamp_uses_utc_amz_format.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::crypto::{hex_lower, hmac_sha256, hmac_sha256_hex, sha256_hex}`
- `lpe_domain::utc_from_unix_seconds`
- `reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH},
    Client, Method, StatusCode, Url,
}`
- `serde_json::{json, Map, Value}`
- `std::{
    collections::BTreeMap,
    env, fmt,
    time::{SystemTime, UNIX_EPOCH},
}`
- `uuid::Uuid`
- `crate::StoragePoolConfigSummary`
- `super::*`
- `serde_json::json`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)