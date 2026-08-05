---
type: Rust Module
title: core
resource: crates/lpe-storage/src/core.rs#L1-L1082
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-context-result
  - external/sqlx-postgres-pgconnectoptions-pgpooloptions-pool-postgres
  - external/crate-expected-schema-version
  - external/std-env-str-fromstr
  - external/anyhow-context-result
  - external/sqlx-postgres-pgconnectoptions-pgpooloptions
  - external/uuid-uuid
  - external/super-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [Storage](../../../../classes/crates/lpe-storage/src/core/Storage.md)
- [new](../../../../functions/crates/lpe-storage/src/core/Storage/new.md)
- [connect](../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [pool](../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [database_url](../../../../functions/crates/lpe-storage/src/core/Storage/database_url.md)
- [assert_schema_version](../../../../functions/crates/lpe-storage/src/core/Storage/assert_schema_version.md)
- [assert_required_schema_objects](../../../../functions/crates/lpe-storage/src/core/Storage/assert_required_schema_objects.md)
- [startup_rejects_tagged_schema_without_required_mapi_shape](../../../../functions/crates/lpe-storage/src/core/startup_rejects_tagged_schema_without_required_mapi_shape.md)
- [startup_uses_canonical_public_schema_when_search_path_has_shadow_schema](../../../../functions/crates/lpe-storage/src/core/startup_uses_canonical_public_schema_when_search_path_has_shadow_schema.md)
- [connect_pins_search_path_to_canonical_public_schema](../../../../functions/crates/lpe-storage/src/core/connect_pins_search_path_to_canonical_public_schema.md)

# Imports

- `anyhow::{bail, Context, Result}`
- `sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Pool, Postgres,
}`
- `crate::EXPECTED_SCHEMA_VERSION`
- `std::{env, str::FromStr}`
- `anyhow::{Context, Result}`
- `sqlx::postgres::{PgConnectOptions, PgPoolOptions}`
- `uuid::Uuid`
- `super::Storage`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)