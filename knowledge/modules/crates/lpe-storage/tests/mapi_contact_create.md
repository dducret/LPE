---
type: Rust Module
title: mapi_contact_create
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L1-L713
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-env-str-fromstr-sync-oncelock-time-duration
  - external/anyhow-context-result
  - external/lpe-storage-canonicalchangecategory-contactnamefields-contactsourcefields-mapicontactcreateinput-mapicontactcustompropertyvalue-mapicontactimportdisposition-mapicontactimportobjectdeleted-mapicontactimportedidentity-storage-upsertclientcontactinput
  - external/serde-json-json
  - external/sqlx-postgres-pgconnectoptions-pgpooloptions-pgpool-row
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [database_test_lock](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/database_test_lock.md)
- [ContactFixture](../../../../classes/crates/lpe-storage/tests/mapi_contact_create/ContactFixture.md)
- [TestSchemaCleanup](../../../../classes/crates/lpe-storage/tests/mapi_contact_create/TestSchemaCleanup.md)
- [armed](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/TestSchemaCleanup/armed.md)
- [disarm](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/TestSchemaCleanup/disarm.md)
- [drop](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/TestSchemaCleanup/drop/drop.md)
- [cleanup](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/ContactFixture/cleanup.md)
- [contact_fixture](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/contact_fixture.md)
- [imported_identity](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/imported_identity.md)
- [contact_input](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/contact_input.md)
- [create_input](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/create_input.md)
- [mapi_contact_create_is_atomic_and_preserves_reserved_import_identity](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/mapi_contact_create_is_atomic_and_preserves_reserved_import_identity.md)
- [mapi_store_id](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/mapi_store_id.md)
- [source_key](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/source_key.md)
- [predecessor_change_list](../../../../functions/crates/lpe-storage/tests/mapi_contact_create/predecessor_change_list.md)

# Imports

- `std::{env, str::FromStr, sync::OnceLock, time::Duration}`
- `anyhow::{Context, Result}`
- `lpe_storage::{
    CanonicalChangeCategory, ContactNameFields, ContactSourceFields, MapiContactCreateInput,
    MapiContactCustomPropertyValue, MapiContactImportDisposition, MapiContactImportObjectDeleted,
    MapiContactImportedIdentity, Storage, UpsertClientContactInput,
}`
- `serde_json::json`
- `sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Row,
}`
- `uuid::Uuid`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)