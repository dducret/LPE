# JMAP Interoperability Matrix

## Current State/Functionality Overview

The JMAP matrix defines checks for canonical Mail, Contacts, Calendar, and Task behavior through the JMAP adapter. It is scoped to implemented JMAP methods and canonical state guarantees.

## Implementation/Usage

- Validate session discovery.
- Validate upload and download.
- Validate mail get/query/changes/queryChanges/set/copy/import/submission behavior.
- Validate contacts get/query/changes/queryChanges/set behavior.
- Validate calendar get/query/changes/queryChanges/set behavior.
- Validate tasks and task lists where published.
- Validate shared and delegated mailbox behavior through canonical grants.
- Treat `Bcc` exposure in user search or AI-facing projections as a failure.
- Validate state tokens are account/method scoped and invalid tokens are rejected.
- Validate `hasMoreChanges` pagination returns resumable intermediate states for `changes` and `queryChanges`.
- Validate WebSocket push resumes from `pushState`, replays the canonical journal when possible, and falls back to a full snapshot when replay is truncated.

## Reference Table/List

| Case | Requirement |
| --- | --- |
| Mail read | `Email/get`, `Email/query`, `Email/changes`, `Email/queryChanges` |
| Mail write | `Email/set`, `Email/copy`, `Email/import` |
| Submission | `EmailSubmission/set` creates canonical `Sent` |
| Blob flow | upload and download round trip |
| Contacts | `ContactCard/get`, `ContactCard/query`, `ContactCard/set` |
| Calendar | `CalendarEvent/get`, `CalendarEvent/query`, `CalendarEvent/set` |
| Tasks | `TaskList/get`, `Task/get`, `Task/query`, `Task/set` where exposed |
| Shared state | owner and grantee see canonical changes |
| State safety | `oldState`, `newState`, and `queryState` are scoped to account, method, filter, and sort |
| Push reconnect | `WebSocketPushEnable` with previous `pushState` reports missed canonical changes or confirms cursor advancement |
| Push fallback | stale, invalid, or truncated journal replay produces a full current state snapshot |

| Harness | Purpose |
| --- | --- |
| `tools/jmap_tester_big_three_check.pl` | Mail/Contacts/Calendar JMAP validation |
| `tools/jmap_live_shared_delegated_check.py` | shared/delegated canonical state validation |

| Local test focus | Required behavior |
| --- | --- |
| `crates/lpe-jmap::state` | state and query-change pagination, reorders, token validation |
| `websocket_push_states_include_shared_mailbox_accounts` | shared mailbox accounts participate in push state |
| `websocket_reconnect_recovers_task_changes_from_canonical_journal` | reconnect replay reports missed task changes |
| `websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal` | delegated rights changes wake grantees |
| `websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated` | truncated replay is safe and complete |
