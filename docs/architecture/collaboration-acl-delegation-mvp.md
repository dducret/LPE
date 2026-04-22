# Collaboration ACL and Delegation MVP

### Objective

This document describes the first `LPE` MVP sharing, delegation, and fine ACL model for contacts, calendars, task lists, and mailbox delegation.

The MVP stays strictly aligned with the existing canonical tables:

- `contacts`
- `calendar_events`
- `task_lists`
- `tasks`
- `audit_events`

It does not introduce any parallel business-object storage. Rights are added as canonical layers over the objects already owned by an account.

### Principles

- contacts and events remain stored only in their existing canonical tables
- task lists and tasks remain stored only in their canonical task tables
- sharing and delegation stay limited to accounts inside the same tenant
- the rights model is shared by `JMAP`, `DAV`, the web client, and account APIs
- no protocol creates its own sharing model
- mailbox delegation and sender authorization stay canonical and are enforced in the shared submission path
- no `Stalwart` code is reused
- rights changes reuse the canonical `audit_events` journal

### Canonical MVP collection model

Each account owns two canonical default collections:

- its default `contacts` collection
- its default `calendar` collection

The MVP does not yet create arbitrary persistent collections. It only exposes:

- the owned `default` collection for the owner
- shared collections derived from grants for other accounts in the same tenant

Objects remain physically stored with their owner. A shared collection is therefore the owner's canonical collection exposed through durable rights, not a copy or adapter-local construct.

### Grant model

The MVP introduces canonical grant tables:

- `collaboration_collection_grants` for default contacts and calendar collections
- `task_list_grants` for canonical task lists

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

For task lists, the uniqueness boundary is:

- `(tenant_id, task_list_id, owner_account_id, grantee_account_id)`

### MVP semantics

The MVP supports:

- calendar sharing between accounts in the same tenant
- contact sharing between accounts in the same tenant
- task-list sharing between accounts in the same tenant
- minimal read/write/delete/share delegation over the full collection
- minimal read/write/delete/share delegation over the full canonical task list
- organizer and attendee-status interoperability on shared calendar collections through the same canonical event rows
- coherent exposure of the same rights through `JMAP` and `DAV`
- minimal audit of grant changes
- shared mailbox projection and delegated sender authorization through the canonical mailbox and submission models

The MVP does not yet support:

- per-item ACLs
- cross-tenant sharing
- sharing groups
- complex secretary or proxy delegation roles beyond full-mailbox projection plus `send-as` and `send-on-behalf`
- partial subscriptions or subset filtering
- fine-grained ACL sync history

### Protocol exposure

#### Mailbox delegation

Mailbox delegation is modeled separately from contacts/calendar collection grants.

The MVP introduces two canonical grant tables:

- `mailbox_delegation_grants` for shared mailbox access
- `sender_delegation_grants` for sender authorization

The mailbox projection grant exposes the delegated mailbox as another canonical mailbox account to the grantee. It does not copy mailbox state, messages, folders, or drafts.

Sender grants are distinct durable rights:

- `send_as`
- `send_on_behalf`

`IMAP ACL` admin commands are an adapter projection over those same canonical grants. The current `IMAP` slice does not create a per-mailbox ACL database or a protocol-local rights journal. Instead:

- mailbox access rights are projected from `mailbox_delegation_grants`
- `p` maps to canonical `send_as`
- `b` is reserved by `LPE` as an `IMAP ACL` projection of canonical `send_on_behalf`
- `SETACL` and `DELETEACL` update the canonical delegation tables directly
- delegated mailbox projection through `IMAP` remains deferred even though the rights are already canonical

Submission always flows through the same canonical `submit_message` path. That path resolves:

- the mailbox owner account
- the authenticated submitting account
- the durable sender behavior recorded on the message row

The canonical message record therefore distinguishes:

- self-send
- `Send As`
- `Send on Behalf`

`Send on Behalf` persists both the delegated `From` mailbox and the authenticated `Sender`. `Send As` persists the delegated `From` mailbox without a separate sender identity.

#### JMAP

`JMAP Contacts` and `JMAP Calendars` expose:

- the authenticated account's `default` collection
- accessible shared collections through `AddressBook/*` and `Calendar/*`
- `myRights` values derived from canonical grants
- organizer and attendee status remain part of the same canonical shared event record; no separate `JMAP` delegation state exists

`ContactCard/set` and `CalendarEvent/set` may create into a shared collection when `may_write=true`.

Mail `JMAP` additionally exposes accessible shared mailbox accounts in the session account map and exposes delegated sender identities through `Identity/get`. Session account read-only state, mailbox `myRights`, and draft submission exposure must be derived from the canonical mailbox delegation grant plus the canonical sender delegation grants, not from adapter-local assumptions.

For delegated mailboxes:

- mailbox visibility comes from `mailbox_delegation_grants`
- draft submission visibility comes from `sender_delegation_grants`
- a shared mailbox without `send-as` or `send-on-behalf` must not be advertised as submittable through `Mailbox/get`
- `Identity/get` may expose `LPE`-specific metadata that makes the canonical sender mode explicit, such as the delegated authorization kind and the authenticated sender identity used for `send-on-behalf`

Draft creation and submission for shared mailboxes keep using the canonical mailbox owner account plus the authenticated submitting account.

`JMAP Tasks` exposes:

- the authenticated account's owned canonical task lists
- accessible shared task lists through `TaskList/*`
- tasks from both owned and shared lists through `Task/*`
- `myRights` on `TaskList` derived from canonical `task_list_grants`
- canonical share-grant changes and shared-task updates now wake the affected `JMAP` principals through the shared canonical push channel instead of relying on owner-local polling assumptions

Task creation and updates may target a shared canonical task list when `may_write=true`. Task deletion requires `may_delete=true`. Task-list rename and destroy remain owner-only operations.

#### DAV

`CardDAV` and `CalDAV` expose:

- `/dav/addressbooks/me/{collection-id}/`
- `/dav/calendars/me/{collection-id}/`

DAV home `PROPFIND` depth `1` returns every accessible collection.

DAV reads and writes apply the same canonical grants as `JMAP`, including organizer and attendee-status updates on accessible shared calendar collections.

`CalDAV` task exposure also applies the same canonical task-list grants as `JMAP`. DAV task collections are projections of canonical task lists, not DAV-local ACL objects.

The mailbox delegation lot does not add mailbox access through `DAV`.

### MVP audit

The MVP does not add a specialized journal. It reuses `audit_events`.

The minimally audited actions are:

- share-grant creation or update
- share-grant deletion
- task-list grant creation or update
- task-list grant deletion
- mailbox delegation grant creation or update
- mailbox delegation grant deletion
- sender delegation grant creation or update
- sender delegation grant deletion

The modified-object detail intentionally remains small in this lot.

### Intentional MVP limits

- collection-only granularity on the canonical default collections
- no user-specific renaming of shared collections
- no sophisticated multi-master conflict handling
- no durable per-right historical sync journal beyond the current canonical push wakeups


