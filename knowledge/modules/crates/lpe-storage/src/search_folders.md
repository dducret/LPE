---
type: Rust Module
title: search_folders
resource: crates/lpe-storage/src/search_folders.rs#L1-L539
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/serde-serialize
  - external/serde-json-value
  - external/sqlx-postgres-row
  - external/uuid-uuid
  - external/crate-canonicalchangecategory-searchfolderrow-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [SearchFolderDefinition](../../../../classes/crates/lpe-storage/src/search_folders/SearchFolderDefinition.md)
- [UpsertSearchFolderInput](../../../../classes/crates/lpe-storage/src/search_folders/UpsertSearchFolderInput.md)
- [BuiltinSearchFolderDefinition](../../../../classes/crates/lpe-storage/src/search_folders/BuiltinSearchFolderDefinition.md)
- [exchange_builtin_search_folder_definitions](../../../../functions/crates/lpe-storage/src/search_folders/exchange_builtin_search_folder_definitions.md)
- [map_search_folder](../../../../functions/crates/lpe-storage/src/search_folders/map_search_folder.md)
- [validate_search_folder_input](../../../../functions/crates/lpe-storage/src/search_folders/validate_search_folder_input.md)
- [fetch_search_folders](../../../../functions/crates/lpe-storage/src/search_folders/Storage/fetch_search_folders.md)
- [fetch_search_folders_by_ids](../../../../functions/crates/lpe-storage/src/search_folders/Storage/fetch_search_folders_by_ids.md)
- [upsert_search_folder](../../../../functions/crates/lpe-storage/src/search_folders/Storage/upsert_search_folder.md)
- [delete_search_folder](../../../../functions/crates/lpe-storage/src/search_folders/Storage/delete_search_folder.md)
- [ensure_exchange_search_folders](../../../../functions/crates/lpe-storage/src/search_folders/Storage/ensure_exchange_search_folders.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `serde::Serialize`
- `serde_json::Value`
- `sqlx::{Postgres, Row}`
- `uuid::Uuid`
- `crate::{CanonicalChangeCategory, SearchFolderRow, Storage}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)