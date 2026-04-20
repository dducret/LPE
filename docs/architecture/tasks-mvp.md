# Tasks MVP

### Objective

This document describes the first canonical tasks and to-do model implemented in `LPE`.

The MVP adds one internal business model for personal account-scoped tasks without introducing any protocol-specific parallel logic. Future `JMAP Tasks`, `DAV`, and mobile adapters must reuse the same store and access rules.

### Architectural principles

- `PostgreSQL` remains the primary store
- tasks are stored in one canonical `tasks` table
- each task is owned by one `account_id`
- MVP rights are enforced through the authenticated mailbox account; no parallel rights model is introduced
- the internal API writes directly to the canonical model
- no `Stalwart` code is reused

### MVP data model

The `tasks` table exposes the following fields:

- `id`: canonical `UUID`
- `tenant_id`: internal multi-tenant scope
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
- a storage base compatible with a future `DAV` `VTODO` layer
- future mobile and `ActiveSync` reuse without reshaping the storage model
- future incremental synchronization through `updated_at` and `sort_order`

### Out of scope for the MVP

- shared task lists
- cross-account delegation
- recurrence
- subtasks
- task attachments
- reminders and alarms
- exposed `JMAP Tasks`, `VTODO`, or `ActiveSync Tasks` protocol adapters


