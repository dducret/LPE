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
  - internationalized mailbox names use the shared policy in `docs/architecture/internationalized-mailbox-names.md`; IMAP uses IMAP4rev2 UTF-8 behavior, and `UTF8=ACCEPT` is included in the first internationalized mailbox release but must be advertised only when command handling and tests match that behavior
  - `CAPABILITY` advertises `IMAP4rev2` and `UTF8=ACCEPT`; clients can enable UTF-8 quoted mailbox names with `ENABLE UTF8=ACCEPT`
  - `LIST`, `LSUB`, `STATUS`, and mailbox-name responses render mailbox names through the shared IMAP mailbox-name serializer; non-ASCII response names are sent as literals until `UTF8=ACCEPT` is enabled, then as UTF-8 quoted strings
  - `SUBSCRIBE`, `UNSUBSCRIBE`, and `LSUB` use canonical persisted `mailbox_subscriptions` state shared with JMAP `isSubscribed`; `LSUB` lists only currently subscribed mailboxes that match the requested pattern
  - mailbox name validation follows the strict Unicode policy in `docs/architecture/internationalized-mailbox-names.md`, including NFC display storage, canonical-key sibling collision checks, reserved-name protection, `/` hierarchy delimiter rules, and rejection of mixed-script and confusable names
  - `CREATE`, `RENAME`, `DELETE`, `SUBSCRIBE`, `UNSUBSCRIBE`, `STATUS`, `SELECT`, `EXAMINE`, `APPEND`, `COPY`, and `MOVE` resolve mailbox names through the shared decoded IMAP mailbox path parser
  - `LIST` wildcard matching runs over decoded UTF-8 mailbox names; `%` matches one hierarchy segment and `*` matches recursively across `/` delimiters
  - standard mailbox names such as `INBOX`, `Sent`, and `Trash` remain canonical backend names; localization is client UI presentation driven by special-use attributes, not by storing translated role names
  - reserved special-use names and compatibility aliases such as `INBOX`, `Sent Items`, and `Deleted Items` resolve only by canonical mailbox role; a custom mailbox whose display name collides with a reserved alias must not be treated as that special-use mailbox
  - `APPEND` uses canonical draft or import persistence
  - flags update canonical message state
  - search uses canonical indexed fields and must not expose `Bcc`
  - UID behavior is scoped to mailbox state
- File validation:
  - any imported client-provided file must pass the canonical validation path
- Edge exposure:
  - public IMAP is exposed as IMAPS through `LPE-CT`, not by making the core `LPE` IMAP listener public
  - autoconfiguration may publish IMAP only when `LPE-CT` exposes the IMAPS proxy and `LPE_AUTOCONFIG_IMAP_HOST` names that public edge endpoint
  - autoconfiguration must not publish client `SMTP` from IMAP support; SMTP appears only when `LPE-CT` exposes real authenticated client submission
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
| Public IMAPS | `LPE-CT` TLS proxy to private core `lpe-imap` |
| Autoconfig IMAP | explicit `LPE_AUTOCONFIG_IMAP_HOST` pointing at the public `LPE-CT` IMAPS endpoint |

## LIST Hierarchy Examples

With `/` as the hierarchy delimiter, `LIST "" "%"` returns only selectable
mailboxes one level below the root, such as `INBOX`, `Sent`, `Projects`, or
`案件`. It does not return `Projects/Alpha` or `案件/顧客`.

`LIST "" "Projects/%"` returns direct children such as `Projects/Alpha` and
`Projects/Beta`, but not `Projects/Alpha/Q1`.

`LIST "" "Projects/*"` matches recursively and may return `Projects/Alpha`,
`Projects/Beta`, and `Projects/Alpha/Q1`.

Wildcard matching is performed after mailbox names are decoded to Unicode
hierarchy paths. Compatibility aliases such as `Deleted Items` match only
documented special-use aliases and are not emitted as extra mailbox names.

| Transcript | Local coverage |
| --- | --- |
| UTF-8 mailbox lifecycle and `UTF8=ACCEPT` | `crates/lpe-imap::tests::utf8_accept_enables_utf8_mailbox_response_quoting`, `crates/lpe-imap::tests::unicode_mailbox_commands_resolve_and_render_consistently`, `crates/lpe-imap::tests::unicode_nested_paths_and_list_wildcards_work_by_segment`, `crates/lpe-imap::tests::malformed_utf8_mailbox_paths_are_rejected` |
| Outlook first login/list/select/sync | `crates/lpe-imap::tests::outlook_first_login_list_select_sync_transcript` |
| Outlook UID refresh | `crates/lpe-imap::tests::outlook_uid_search_refreshes_selected_mailbox_before_fetch` |
| Outlook large mailbox refresh | `crates/lpe-imap::tests::outlook_large_mailbox_refresh_keeps_uid_fetch_and_search_stable` |
| Outlook setup probes | `crates/lpe-imap::tests::quota_probe_commands_are_tolerated_for_outlook_setup` |
| Thunderbird copy to trash and expunge | `crates/lpe-imap::tests::thunderbird_copy_to_trash_then_expunge_removes_source_only` |
| Thunderbird draft delete by move | `crates/lpe-imap::tests::thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy` |
| CONDSTORE flags | `crates/lpe-imap::tests::condstore_store_reports_modified_and_keeps_fresh_messages` |
| IDLE refresh | `crates/lpe-imap::tests::idle_reports_selected_mailbox_flag_changes` |
