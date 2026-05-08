# IMAP Adapter

## Current State/Functionality Overview

`lpe-imap` exposes IMAP as a mailbox compatibility layer over canonical `LPE` mailbox state. It supports mailbox access and mutation without adding IMAP-specific mailbox, `Sent`, `Drafts`, or `Outbox` state.

## Implementation/Usage

- Authentication:
  - mailbox account credentials
  - supported mailbox auth tokens where configured
- Supported commands:
  - `LOGIN`
  - `CAPABILITY`
  - `NOOP`
  - `LOGOUT`
  - `LIST`
  - `XLIST`
  - `STATUS`
  - `CREATE`
  - `DELETE`
  - `RENAME`
  - `SELECT`
  - `EXAMINE`
  - `CHECK`
  - `CLOSE`
  - `UNSELECT`
  - `EXPUNGE`
  - `FETCH`
  - `UID FETCH`
  - `STORE`
  - `UID STORE`
  - `SEARCH`
  - `UID SEARCH`
  - `COPY`
  - `UID COPY`
  - `APPEND`
- Canonical behavior:
  - folder aliases such as `Deleted Items` and `Trash` converge on canonical trash
  - `APPEND` uses canonical draft or import persistence
  - flags update canonical message state
  - search uses canonical indexed fields and must not expose `Bcc`
  - UID behavior is scoped to mailbox state
- File validation:
  - any imported client-provided file must pass the canonical validation path
- Diagnostics:
  - expose enough session, command, and mailbox identifiers for traceability
  - avoid logging protected metadata or secrets
- Compatibility transcripts:
  - Outlook desktop first-login flow must cover `CAPABILITY`, `ID`, `LOGIN`, `ENABLE CONDSTORE`, `LIST`, `XLIST`, `EXAMINE`, `SELECT`, `STATUS`, `UID FETCH`, `UID SEARCH`, and partial body fetch behavior
  - Thunderbird delete flows must cover copy-or-move to canonical trash, `\Deleted` flags, `UID EXPUNGE`, and draft deletion without leaving duplicate canonical drafts
  - `IDLE` and `CONDSTORE` behavior must remain stable for mailbox refresh and flag changes

## Reference Table/List

| Area | Rule |
| --- | --- |
| Role | compatibility mailbox access |
| Storage | canonical `LPE` mailbox tables |
| `Sent` | canonical only |
| `Drafts` | canonical only |
| `Outbox` | no protocol-local implementation |
| Search | no `Bcc` exposure |
| External `SMTP` | `LPE-CT`, not IMAP adapter |

| Transcript | Local coverage |
| --- | --- |
| Outlook first login/list/select/sync | `crates/lpe-imap::tests::outlook_first_login_list_select_sync_transcript` |
| Outlook UID refresh | `crates/lpe-imap::tests::outlook_uid_search_refreshes_selected_mailbox_before_fetch` |
| Outlook setup probes | `crates/lpe-imap::tests::quota_probe_commands_are_tolerated_for_outlook_setup` |
| Thunderbird copy to trash and expunge | `crates/lpe-imap::tests::thunderbird_copy_to_trash_then_expunge_removes_source_only` |
| Thunderbird draft delete by move | `crates/lpe-imap::tests::thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy` |
| CONDSTORE flags | `crates/lpe-imap::tests::condstore_store_reports_modified_and_keeps_fresh_messages` |
| IDLE refresh | `crates/lpe-imap::tests::idle_reports_selected_mailbox_flag_changes` |
