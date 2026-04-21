# Tasks MVP

### Objective

This document describes the current canonical tasks and to-do model implemented in `LPE`.

The first increment added one internal business model for personal account-scoped tasks without introducing any protocol-specific parallel logic. The current increment keeps that rule and extends it with canonical multi-list support. Future `JMAP Tasks`, `DAV`, and mobile adapters must reuse the same store and access rules.

### Architectural principles

- `PostgreSQL` remains the primary store
- task lists are stored in one canonical `task_lists` table
- tasks are stored in one canonical `tasks` table and always belong to one canonical task list
- each task list and task is owned by one `account_id`
- MVP rights are enforced through the authenticated mailbox account; no parallel rights model is introduced
- the internal API writes directly to the canonical model
- no `Stalwart` code is reused

### Canonical data model

The `task_lists` table exposes the following fields:

- `id`: canonical `UUID`
- `tenant_id`: internal multi-tenant scope
- `account_id`: owning account and current rights boundary
- `name`: required user-facing list name
- `role`: optional canonical role; the default owner list uses `inbox`
- `sort_order`: stable list ordering for future clients
- `created_at`, `updated_at`: tracking metadata and future sync anchors

Each account has one canonical default task list:

- role: `inbox`
- initial name: `Tasks`
- it is created in canonical storage, not synthesized in a protocol adapter

The `tasks` table exposes the following fields:

- `id`: canonical `UUID`
- `tenant_id`: internal multi-tenant scope
- `task_list_id`: canonical parent task list
- `account_id`: owning account and MVP rights boundary
- `title`: required user-facing title
- `description`: free-text description
- `status`: `needs-action`, `in-progress`, `completed`, `cancelled`
- `due_at`: optional `TIMESTAMPTZ` due date
- `completed_at`: optional `TIMESTAMPTZ` completion timestamp
- `sort_order`: stable presentation ordering for future clients
- `created_at`, `updated_at`: tracking metadata and future sync anchors

The status set is intentionally aligned with a future `VTODO` mapping and remains reusable for later `JMAP Tasks` and mobile clients.

### MVP internal API

The following account-scoped endpoints are exposed by `lpe-admin-api`:

- `GET /api/mail/tasks`
- `GET /api/mail/tasks/{task_id}`
- `POST /api/mail/tasks`
- `DELETE /api/mail/tasks/{task_id}`

`POST /api/mail/tasks` is the MVP upsert endpoint:

- create when `id` is absent
- update when `id` is present and belongs to the authenticated account
- when `task_list_id` is absent, the canonical default task list is used

The `/api/mail/workspace` payload now also includes `tasks` so clients can load one unified mailbox and collaboration workspace snapshot.

### MVP rules

- title is required
- an empty status is normalized to `needs-action`
- `completed_at` is retained only for `completed`
- when a task moves to `completed` without an explicit completion timestamp, `LPE` fills `completed_at`
- read, write, and delete operations are always scoped to the authenticated account

### Future adapter preparation

The MVP explicitly prepares:

- a canonical projection reusable by future `JMAP Tasks`
- a storage base reusable by the first `DAV` `VTODO` layer without a DAV-specific store
- future mobile and `ActiveSync` reuse without reshaping the storage model
- future incremental synchronization through `updated_at` and `sort_order`

### JMAP Tasks adapter current increment

The first `JMAP Tasks` adapter now ships in `lpe-jmap`.

It remains a thin adapter above the canonical `task_lists` and `tasks` tables:

- the canonical `task_lists` and `tasks` rows remain the single source of truth
- no `JMAP`-specific task store, sync table, or rights model is introduced
- all reads and writes remain scoped to the authenticated account
- the exposed shape stays close to future `DAV` `VTODO` and `ActiveSync Tasks` reuse

#### Task list model

The current increment exposes canonical `TaskList` objects per authenticated account:

- one canonical default list with role `inbox`
- zero or more additional owner-only personal lists
- `TaskList/set` now creates, updates, and destroys canonical task lists
- the default list may be renamed and reordered but not destroyed
- destroying a non-default list is allowed only when the list is empty

Task-list grants and shared task lists remain deferred. This increment deliberately stops before introducing cross-account task rights.

#### Canonical mapping

The first `Task` mapping is:

- `Task.id` and `Task.uid` -> canonical `tasks.id`
- `Task.taskListId` -> canonical `task_lists.id`
- `Task.title` -> `tasks.title`
- `Task.description` -> `tasks.description`
- `Task.status` -> `tasks.status`
- `Task.due` -> `tasks.due_at` as a UTC timestamp string
- `Task.completed` -> `tasks.completed_at` as a UTC timestamp string
- `Task.sortOrder` -> `tasks.sort_order`
- `Task.updated` -> `tasks.updated_at` as a UTC timestamp string

The MVP keeps the canonical task status set unchanged:

- `needs-action`
- `in-progress`
- `completed`
- `cancelled`

This preserves a direct bridge to future `VTODO` `STATUS` mapping and to later mobile compatibility layers.

### DAV `VTODO` adapter MVP

The first `DAV` task adapter now ships in `lpe-dav`.

It remains a thin compatibility layer above the canonical `task_lists` and `tasks` tables:

- the canonical `task_lists` and `tasks` rows remain the single source of truth
- there is no DAV-specific task table, sync state, or rights store
- mailbox-account authentication is reused directly
- MVP task rights remain bounded to the authenticated account only
- the `DAV` layer exposes one owner-only `VTODO` collection and does not introduce shared task collections

#### DAV collection model

The current DAV increment still exposes one canonical task collection per authenticated account:

- collection path: `/dav/calendars/me/tasks/`
- collection display name: `Tasks`
- collection component set: `VTODO`
- collection rights: read, create, update, and delete for the authenticated account only

The task collection is intentionally separate from the default `VEVENT` collection so the first interoperability layer stays predictable for clients and for future protocol adapters.

The DAV adapter currently maps that collection to the canonical default task list only. Additional canonical task lists already exist in storage for future DAV collection expansion, but DAV multi-list publication is intentionally deferred in this increment.

#### Canonical mapping

The first `tasks` to `VTODO` mapping is:

- `tasks.id` -> `UID`
- `tasks.title` -> `SUMMARY`
- `tasks.description` -> `DESCRIPTION`
- `tasks.status` -> `STATUS` using `needs-action -> NEEDS-ACTION`, `in-progress -> IN-PROCESS`, `completed -> COMPLETED`, `cancelled -> CANCELLED`
- `tasks.due_at` -> `DUE` as a UTC timestamp
- `tasks.completed_at` -> `COMPLETED` as a UTC timestamp
- `tasks.updated_at` -> `LAST-MODIFIED` as a UTC timestamp
- `tasks.sort_order` -> `X-LPE-SORT-ORDER`

The reverse mapping follows the same contract:

- `UID` remains the canonical task `id`
- missing `STATUS` is normalized to `needs-action`
- missing `COMPLETED` for a `COMPLETED` task is filled by the canonical task rules, not by DAV storage
- unsupported `VTODO` properties are ignored in the MVP instead of creating parallel canonical fields

#### Implemented DAV task operations

The first adapter implements:

- `PROPFIND` for task-collection discovery and item listing
- `REPORT` for multiget-style `href` targeting, simple text-match filtering, and minimal `time-range` filtering on `DUE`
- `GET` for one `VTODO` resource
- `PUT` for full-resource create or replace of one `VTODO`
- `DELETE` for one task resource
- `ETag`, `If-Match`, and `If-None-Match` handling on task resources

#### Implemented JMAP methods

The first adapter implements:

- `TaskList/get`
- `TaskList/changes`
- `TaskList/set` with canonical create, update, and empty-list destroy semantics
- `Task/get`
- `Task/query`
- `Task/queryChanges`
- `Task/changes`
- `Task/set`

The current increment intentionally does not yet implement:

- shared task-list grants
- shared task lists
- `Task/copy`
- task notifications
- recurrence, alerts, subtasks, assignees, or attachments

#### Sync behavior

The current sync contract is:

- `Task/changes` fingerprints include canonical task content together with `task_list_id`, `sort_order`, and `updated_at`
- `Task/query` is ordered by canonical task-list `sort_order`, then task `sort_order`, then `updated_at`, then `id`
- `Task/queryChanges` treats task reordering, task-list moves, and task-list ordering changes as ordered-result changes so clients can reconcile list-aware task ordering
- `TaskList/changes` uses canonical task-list rows directly; there is no parallel list sync store

### Out of scope for the MVP

- shared task-list grants
- shared task lists
- cross-account delegation
- recurrence
- subtasks
- task attachments
- reminders and alarms
- shared DAV task collections
- partial `VTODO` patch semantics
- alarms, organizers, attendees, recurrence, and scheduling workflows on `VTODO`
- `ActiveSync Tasks`


