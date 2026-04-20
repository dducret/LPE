# Collaboration ACL and Delegation MVP

### Objective

This document describes the first `LPE` MVP sharing, delegation, and fine ACL model for contacts and calendars.

The MVP stays strictly aligned with the existing canonical tables:

- `contacts`
- `calendar_events`
- `audit_events`

It does not introduce any parallel business-object storage. Rights are added as a canonical collection layer over the objects already owned by an account.

### Principles

- contacts and events remain stored only in their existing canonical tables
- sharing and delegation stay limited to accounts inside the same tenant
- the rights model is shared by `JMAP`, `DAV`, the web client, and account APIs
- no protocol creates its own sharing model
- no `Stalwart` code is reused
- rights changes reuse the canonical `audit_events` journal

### Canonical MVP collection model

Each account implicitly owns two canonical collections:

- its default `contacts` collection
- its default `calendar` collection

The MVP does not yet create arbitrary persistent collections. It only exposes:

- the owned `default` collection for the owner
- virtual shared collections derived from grants for other accounts in the same tenant

Objects remain physically stored with their owner. A shared collection is therefore a rights projection over the owner's canonical objects, not a copy.

### Grant model

The MVP introduces a canonical `collaboration_collection_grants` table.

Each grant is scoped by:

- `tenant_id`
- `collection_kind` in `contacts` or `calendar`
- `owner_account_id`
- `grantee_account_id`

Each grant carries the following rights:

- `may_read`
- `may_write`
- `may_delete`
- `may_share`

MVP constraints:

- `owner_account_id` and `grantee_account_id` must stay inside the same tenant
- self-delegation is not allowed
- `may_write`, `may_delete`, and `may_share` imply `may_read`
- `may_delete` and `may_share` also imply `may_write`
- only one grant exists per `(tenant_id, collection_kind, owner_account_id, grantee_account_id)`

### MVP semantics

The MVP supports:

- calendar sharing between accounts in the same tenant
- contact sharing between accounts in the same tenant
- minimal read/write/delete/share delegation over the full collection
- organizer and attendee-status interoperability on shared calendar collections through the same canonical event rows
- coherent exposure of the same rights through `JMAP` and `DAV`
- minimal audit of grant changes

The MVP does not yet support:

- per-item ACLs
- cross-tenant sharing
- sharing groups
- complex secretary or proxy delegation roles
- partial subscriptions or subset filtering
- fine-grained ACL sync history

### Protocol exposure

#### JMAP

`JMAP Contacts` and `JMAP Calendars` expose:

- the authenticated account's `default` collection
- accessible shared collections through `AddressBook/*` and `Calendar/*`
- `myRights` values derived from canonical grants
- organizer and attendee status remain part of the same canonical shared event record; no separate `JMAP` delegation state exists

`ContactCard/set` and `CalendarEvent/set` may create into a shared collection when `may_write=true`.

#### DAV

`CardDAV` and `CalDAV` expose:

- `/dav/addressbooks/me/{collection-id}/`
- `/dav/calendars/me/{collection-id}/`

DAV home `PROPFIND` depth `1` returns every accessible collection.

DAV reads and writes apply the same canonical grants as `JMAP`, including organizer and attendee-status updates on accessible shared calendar collections.

### MVP audit

The MVP does not add a specialized journal. It reuses `audit_events`.

The minimally audited actions are:

- share-grant creation or update
- share-grant deletion

The modified-object detail intentionally remains small in this lot.

### Intentional MVP limits

- collection-only granularity on the implicit default collections
- no user-specific renaming of shared collections
- no `send-on-behalf`, `send-as`, or shared-mailbox workflow delegation
- no sophisticated multi-master conflict handling
- no real-time rights-change notification


