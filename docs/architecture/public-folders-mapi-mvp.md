# Public Folders and MAPI Per-User State

## Current State/Functionality Overview

Public folders now have canonical `LPE` storage, authenticated mail APIs,
permission rows, per-user read/unread rows, replay facts, and tombstones.
MAPI/HTTP public-folder logon and replica ROPs remain guarded protocol work;
they must not create protocol-local public-folder state. Bounded EWS folder and
item projection may expose public-folder data only through the canonical tables
described here.

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
- `GET /api/mail/public-folders/{folderId}`
- `GET /api/mail/public-folders/{folderId}/children`
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

Per-user read/unread is private to the authenticated account unless an explicit
future administrative audit/export flow is documented. It must never be modeled
as a shared item mutation. Public-folder item changes and per-user read-state
changes therefore need separate replay facts so MAPI, JMAP, DAV, ActiveSync, and
future web clients can sync item content and user-private state independently.

MAPI/HTTP support can begin from the canonical API and replay model above. The
first ROP mapping should be read-only tree discovery and item sync.
`RopGetPerUserLongTermIds`, `RopGetPerUserGuid`,
`RopReadPerUserInformation`, and `RopWritePerUserInformation` may map only to
`public_folder_per_user_state`; they must not create Exchange-compatible binary
per-user blobs or protocol-local state. Public-folder replica ROPs may expose
only documented single-server or cluster metadata that exists in canonical LPE
state.

## Reference Table/List

| Area | Canonical source | MAPI/HTTP rule |
| --- | --- | --- |
| Public-folder tree | `public_folder_trees`, `public_folders` | No public-folder logon or hierarchy ROP until durable tree state exists. |
| Public-folder items | `public_folder_items` plus canonical message/body/blob tables where applicable | No opaque Exchange message blobs. |
| Permissions | `public_folder_permissions` | Same-tenant grants only; no MAPI-local ACLs. |
| Per-user read/unread | `public_folder_per_user_state` | User-private state only; item content change log is separate. |
| Replay | `public_folder_change_log` and public-folder tombstones, or proven generic equivalents | Must support permission revocation, item deletion, and per-user-state deltas. |
| ROP enablement | Architecture, SQL, API, replay tests, then ROP tests | Do not implement public-folder ROPs before the canonical layer exists. |
