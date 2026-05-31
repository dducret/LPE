# Public Folders and MAPI Per-User State

## Current State/Functionality Overview

Public folders now have canonical `LPE` storage, authenticated mail APIs,
permission rows, per-user read/unread rows, replay facts, and tombstones.
MAPI/HTTP public-folder replica, recipient-bearing item conversion, and
per-user-information blob ROPs remain guarded protocol work; they must not
create protocol-local public-folder state. The first bounded MAPI steps are
enabled: public-folder `RopLogon` now creates a
distinct public-folder store handle, an immediate root hierarchy probe returns
an empty guarded hierarchy instead of leaking private mailbox folders, and a
store-backed hierarchy probe can list canonical public-folder roots from
`public_folders`. Store-backed public-folder hierarchy tables can also traverse
canonical child folders, and normal public-folder contents tables can project
canonical post-like items as read-only MAPI rows. `RopOpenMessage` plus
`RopGetPropertiesSpecific` can read the bounded canonical property projection
for those public-folder posts, and content sync export can emit canonical
public-folder post facts plus canonical per-user read/unread state. Bounded
`RopSetReadFlags` and `RopSetMessageReadFlag` calls on public-folder item
handles update only `public_folder_per_user_state` for the authenticated
account. `RopGetPerUserLongTermIds` and `RopGetPerUserGuid` can perform bounded
public-folder identity lookup from the private mailbox or public-folder logon
handle against canonical public-folder IDs and the LPE replica GUID. Bounded EWS
folder, item projection, item lookup, post creation, item update, and item
deletion may expose or mutate public-folder data only through the canonical
tables described here.

Current bounded EWS coverage includes public-folder `FindFolder`, `GetFolder`,
`SyncFolderHierarchy`, `CreateFolder`, `DeleteFolder`, `FindItem`,
`SyncFolderItems`, `GetItem`, `CreateItem` with `SaveOnly`, `UpdateItem`,
`DeleteItem`, `CopyItem`, and `MoveItem`.
`CopyItem` and `MoveItem` for public-folder posts are public-folder-to-public-
folder only; message-to-public-folder and public-folder-to-mailbox conversion
remain out of scope until a canonical cross-store conversion rule is documented.

## Implementation/Usage

Public folders must be canonical core `LPE` collaboration state, not Exchange
FAI blobs and not MAPI-local state. The first implementation should add explicit
public-folder storage and APIs before enabling any public-folder ROP:

- `public_folder_trees`: tenant-owned tree roots with display name, stable
  canonical id, lifecycle state, and admin ownership.
- `public_folders`: tenant-owned folders with parent id, display name, folder
  class, path metadata, sort order, lifecycle state, and change counters.
- `public_folder_items`: item rows for mail-like posts and collaboration objects
  stored in a public folder. Mail-like items must reference canonical `messages`
  or a dedicated canonical public-folder item body table; they must not be stored
  as opaque MAPI messages.
- `public_folder_permissions`: same-tenant account/group grants for folder
  visibility and mutation.
- `public_folder_per_user_state`: per-account state keyed by
  `(tenant_id, public_folder_id, item_id, account_id)` for read/unread,
  last-seen change, and other explicitly documented user-private facts.
- `public_folder_change_log`: replay rows for folder, item, permission, and
  per-user-state changes. This may be a public-folder-specific category in
  `mail_change_log` only if replay can preserve folder tree scope, per-user
  visibility, and permission revocation without ambiguity.
- `public_folder_tombstones`: deletion rows for folders, items, permissions, and
  per-user-state entries if the generic `tombstones` table cannot preserve the
  required public-folder scope.

The API layer should come before protocol support:

- `GET /api/mail/public-folders/trees`
- `POST /api/mail/public-folders/trees`
- `GET /api/mail/public-folders/{folderId}`
- `PATCH /api/mail/public-folders/{folderId}`
- `DELETE /api/mail/public-folders/{folderId}`
- `GET /api/mail/public-folders/{folderId}/children`
- `POST /api/mail/public-folders/{folderId}/children`
- `GET /api/mail/public-folders/{folderId}/items`
- `POST /api/mail/public-folders/{folderId}/items`
- `PATCH /api/mail/public-folders/{folderId}/items/{itemId}`
- `DELETE /api/mail/public-folders/{folderId}/items/{itemId}`
- `GET /api/mail/public-folders/{folderId}/permissions`
- `PUT /api/mail/public-folders/{folderId}/permissions/{principalId}`
- `DELETE /api/mail/public-folders/{folderId}/permissions/{principalId}`
- `GET /api/mail/public-folders/{folderId}/per-user-state`
- `PATCH /api/mail/public-folders/{folderId}/per-user-state`

These APIs must enforce tenant boundaries and the canonical permission model.
Administrators may create trees and top-level folders. Folder owners or
principals with share rights may manage grants. Read rights allow listing and
reading visible items. Write rights allow creating and updating items. Delete
rights allow item deletion. Share rights allow grant mutation. Owner/admin
rights are required for folder deletion and structural tree mutation.
Initial folder deletion is conservative: root folders cannot be deleted, and a
folder with active child folders or active items must be emptied first.

The EWS adapter may expose public folders with `public-folder:{uuid}` folder
ids and `public-folder-item:{uuid}` item ids. EWS folder creation and deletion,
plus post creation, update, delete, copy, and move, must call the canonical
public-folder storage methods; it must not keep EWS-only folder state, item
state, MIME blobs, ACLs, or read-state facts. Public-folder copy and move are
currently bounded to post-like public-folder items between canonical public
folders. EWS folder deletion inherits the canonical conservative delete rule:
root folders, folders with active child folders, and folders with active items
must not be removed through EWS.

Per-user read/unread is private to the authenticated account unless an explicit
future administrative audit/export flow is documented. It must never be modeled
as a shared item mutation. Public-folder item changes and per-user read-state
changes therefore need separate replay facts so MAPI, JMAP, DAV, ActiveSync, and
future web clients can sync item content and user-private state independently.

MAPI/HTTP support can begin from the canonical API and replay model above.
Current bounded support covers public-folder store logon, root hierarchy
discovery, child-folder hierarchy traversal, and read-only contents-table row
projection plus item open/read and content sync export for canonical post-like
items, including read-state sync export from canonical public-folder item state.
It also covers canonical read-state mutation through `RopSetReadFlags` and
`RopSetMessageReadFlag`, mapped to `public_folder_per_user_state` for the
authenticated account without shared item mutation. `RopGetPerUserLongTermIds`
and `RopGetPerUserGuid` are bounded to canonical public-folder LongTermID and
replica-GUID lookup from private mailbox or public-folder logon handles.
`RopCreateMessage` plus `RopSetProperties`/body streams and
`RopSaveChangesMessage` can create bounded `IPM.Post`-like public-folder items
through `public_folder_items`; recipient-bearing creates remain rejected until a
canonical message-to-public-folder conversion rule is documented. Existing
public-folder post items can be updated through bounded `RopSetProperties` and
body stream writes plus `RopSaveChangesMessage`, with changes persisted directly
to canonical public-folder items. `RopDeleteMessages` and `RopHardDeleteMessages`
delete bounded public-folder post items through the canonical public-folder item
deletion path without introducing MAPI-local dumpster state.
`RopMoveCopyMessages` can copy or move bounded public-folder post items between
canonical public folders by creating a canonical target item and, for moves,
deleting the source item. `RopSynchronizationImportMessageChange` can update
existing bounded public-folder post items and stage new post items that
`RopSaveChangesMessage` persists through the same canonical property mapping.
`RopGetRulesTable` on public-folder handles returns an empty canonical table
because LPE does not define public-folder rules and must not create MAPI-local
rule state. `RopGetMessageStatus` and `RopSetMessageStatus` accept canonical
public-folder item IDs but keep their status bits session-local.
`RopReadPerUserInformation` and
`RopWritePerUserInformation` remain guarded because LPE does not yet define an
Exchange-compatible binary per-user stream over `public_folder_per_user_state`;
they must not create Exchange-compatible binary per-user blobs or protocol-local
state. Public-folder replica ROPs may expose only documented single-server or
cluster metadata that exists in canonical LPE state.

## Reference Table/List

| Area | Canonical source | MAPI/HTTP rule |
| --- | --- | --- |
| Public-folder tree | `public_folder_trees`, `public_folders` | Public-folder logon and hierarchy tables may expose only canonical root and child folder rows. |
| Public-folder items | `public_folder_items` plus canonical message/body/blob tables where applicable | Contents tables, item open/read, content sync export, bounded MAPI post creation, existing-item `SetProperties` updates, existing-item body stream writes, soft/hard item deletion, public-folder-to-public-folder copy/move, and sync-import create/update may project or mutate post-like items; recipient-bearing conversion remains gated; no opaque Exchange message blobs. |
| Permissions | `public_folder_permissions` | Same-tenant grants only; no MAPI-local ACLs. |
| Per-user read/unread | `public_folder_per_user_state` | Content sync may export read/unread state from canonical item rows, bounded MAPI read-flag ROPs may update it for the authenticated account, and per-user LongTermID/GUID lookup may expose canonical public-folder identity only; user-private state only; item content change log is separate. |
| Replay | `public_folder_change_log` and public-folder tombstones, or proven generic equivalents | Must support permission revocation, item deletion, and per-user-state deltas. |
| Rules | none for public folders | `RopGetRulesTable` returns an empty table for public-folder handles; no MAPI-local rules or Sieve rules are attached to public folders. |
| Message status | session-local MAPI compatibility state | `RopGetMessageStatus` and `RopSetMessageStatus` validate public-folder item IDs against canonical items but do not persist item status state. |
| ROP enablement | Architecture, SQL, API, replay tests, then ROP tests | Do not implement public-folder ROPs before the canonical layer exists. |
