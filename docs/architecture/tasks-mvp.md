# Tasks

## Current State/Functionality Overview

`LPE` stores tasks and task lists as canonical account-scoped collaboration data. JMAP Tasks and DAV `VTODO` expose the same model without protocol-specific task storage.

## Implementation/Usage

- Canonical model:
  - `task_lists`
  - `tasks`
  - `task_list_grants`
- Task fields:
  - owner account
  - task list
  - title
  - description
  - status
  - due date
  - priority
  - sort order
  - created/updated timestamps
- Task-list sharing:
  - same-tenant grants only
  - owner retains control
  - grant changes wake affected JMAP principals
- Internal API:
  - `GET /api/mail/task-lists`
  - `PUT /api/mail/task-lists/{task_list_id}/shares`
  - `DELETE /api/mail/task-lists/{task_list_id}/shares/{grantee_account_id}`
  - `GET /api/mail/tasks`
  - `GET /api/mail/tasks/{task_id}`
  - `POST /api/mail/tasks`
  - `DELETE /api/mail/tasks/{task_id}`
  - `/api/mail/workspace` includes tasks
- JMAP Tasks:
  - exposes canonical task lists and tasks
  - supports `TaskList/get`, `TaskList/query`, `TaskList/changes`, `TaskList/queryChanges`, `TaskList/set`, `TaskList/import`, `TaskList/copy`, `Task/get`, `Task/query`, `Task/changes`, `Task/queryChanges`, `Task/set`, `Task/import`, and `Task/copy`
  - treats `TaskList/import`, `TaskList/copy`, `Task/import`, and `Task/copy` as canonical create-style writes using the same payloads as each object's `set` create branch
  - uses canonical `updated_at` and `sort_order`
- DAV `VTODO`:
  - exposes task collections at `/dav/calendars/me/tasks-{task-list-id}/`
  - maps canonical tasks to `VTODO`
  - ignores unsupported `VTODO` properties instead of creating parallel fields

## Reference Table/List

| Status | Meaning |
| --- | --- |
| `needs-action` | open |
| `in-progress` | started |
| `completed` | done |
| `cancelled` | cancelled |

| API | Purpose |
| --- | --- |
| `GET /api/mail/task-lists` | list visible task lists |
| `PUT /api/mail/task-lists/{task_list_id}/shares` | create/update task-list grant |
| `DELETE /api/mail/task-lists/{task_list_id}/shares/{grantee_account_id}` | remove task-list grant |
| `GET /api/mail/tasks` | list visible tasks |
| `GET /api/mail/tasks/{task_id}` | get task |
| `POST /api/mail/tasks` | create/update task |
| `DELETE /api/mail/tasks/{task_id}` | delete task |

| Adapter | Mapping |
| --- | --- |
| JMAP `TaskList` | `task_lists` |
| JMAP `Task` | `tasks` |
| DAV `VTODO` | `tasks` |
