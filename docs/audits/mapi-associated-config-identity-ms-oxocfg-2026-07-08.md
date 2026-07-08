# MAPI Associated Configuration Identity MS-OXOCFG Audit - 2026-07-08

## Scope

This audit checks whether the current `mapi_associated_config_messages` logical
identity of folder, message class, and subject is acceptable for MS-OXOCFG
configuration FAI replay.

Read with:

- `docs/architecture/mapi-over-http-implementation-plan.md`
- `docs/architecture/sql-schema-v2.md`
- `docs/architecture/microsoft-protocol-constants-gap.md`
- `[MS-OXOCFG]` sections 2.2, 2.2.1, 2.2.5.1, 4.1, and 4.2

## Finding

The current logical identity is acceptable for the bounded Outlook
compatibility model LPE implements today.

`mapi_associated_config_messages` is documented as MAPI-only compatibility
state for view, form, and client configuration sync replay. It is not canonical
mailbox content, and it is excluded from JMAP mail, IMAP, user search,
AI-facing projections, and normal mailbox message lists. The schema enforces
one associated configuration row per tenant, account, folder, message class,
and subject, matching the storage implementation's lookup and upsert path.

This key is sufficient for the MS-OXOCFG surfaces currently in scope because
LPE needs to distinguish same-class view/configuration messages by subject,
including `IPM.Microsoft.FolderDesign.NamedView` rows for different view
definitions. The audited gap did not identify an MS-OXOCFG requirement or
current Outlook bootstrap behavior that requires two durable associated
configuration messages with the same folder, message class, and subject but
different hidden-message identities.

## Boundary

No schema or runtime change is required now.

The key must be revisited if real Outlook traces or later Microsoft protocol
coverage show same-folder associated configuration FAI messages that have the
same message class and subject and must survive as distinct rows. At that
point, the minimum change is to add an explicit logical discriminator or
message-instance identity to `mapi_associated_config_messages`, migrate the
unique index, and update the MAPI associated contents and ICS replay paths to
preserve multiple instances without exposing them through canonical mailbox
APIs.
