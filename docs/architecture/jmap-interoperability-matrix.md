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

| Harness | Purpose |
| --- | --- |
| `tools/jmap_tester_big_three_check.pl` | Mail/Contacts/Calendar JMAP validation |
| `tools/jmap_live_shared_delegated_check.py` | shared/delegated canonical state validation |
