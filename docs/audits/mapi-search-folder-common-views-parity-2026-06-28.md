# MAPI Search Folder And Common Views Parity - 2026-06-28

## Scope

This follow-up tracks Outlook Search Folder, Common Views, navigation shortcut,
default-view, and reminder-search parity after the maintenance audit. It is
documentation-only and does not enable additional MAPI behavior.

Read with:

- `docs/architecture/mapi-over-http-implementation-plan.md`
- `docs/architecture/ews-mapi-mvp.md`
- `docs/architecture/outlook-exchange-parity-roadmap.md`
- `docs/architecture/sql-schema-v2.md`
- `docs/architecture/notes-journal-reminders.md`
- `docs/audits/lpe-maintenance-outlook-architecture-audit-2026-06-27.md`

## Current Behavior

LPE models Search Folders as canonical `search_folders` definitions plus
computed hierarchy and content projections. Built-in definitions cover the
Outlook bootstrap surfaces currently modeled by LPE, including To-Do, Tracked
Mail Processing, Contacts Search, and Reminders.

MAPI over HTTP exposes this bounded model as follows:

- Search Folder hierarchy rows project canonical definitions as `FOLDER_SEARCH`
  rows with stable MAPI identities, source keys, and change keys.
- `RopSetSearchCriteria` updates only existing user-saved Search Folders when
  the request maps to bounded canonical JSON.
- `RopGetSearchCriteria` serializes the accepted bounded JSON back into
  parseable MAPI criteria.
- Built-in Search Folders are read-only.
- Common Views search-definition FAI rows are published only when a stored
  `[MS-OXOSRCH]` BLOB has the required advertised `FolderList2` and
  `SearchRestriction` blocks.
- Common Views navigation shortcuts use durable Outlook compatibility metadata
  in `mapi_navigation_shortcuts`; they are not canonical folders.
- Outlook default-view EntryID properties may target bounded Common Views named
  view objects. LPE avoids advertising incomplete folder-local
  `IPM.Microsoft.FolderDesign.NamedView` rows as mailbox state.
- Reminder projection is a computed search-folder surface over canonical
  calendar, task, and message reminder fields. LPE does not have a separate
  reminder object table.

## Remaining Parity Gaps

| Gap | Current boundary | Why it matters to Outlook | Canonical model needed | Tests/evidence needed | Blocks public MAPI autodiscover? |
| --- | --- | --- | --- | --- | --- |
| Full Microsoft Search Folder template BLOB parity | LPE stores canonical JSON definitions and only publishes Common Views SFInfo rows when a valid stored `[MS-OXOSRCH]` BLOB is present. It does not synthesize partial `IPM.Microsoft.WunderBar.SFInfo` rows from LPE-private JSON. | Outlook can persist and replay Search Folder definitions through Common Views FAI messages. Partial or invented SFInfo blobs can confuse cached-mode sync and folder navigation. | A canonical search-definition model that can retain and validate Microsoft Search Folder definition blobs as compatibility metadata while still deriving active results from canonical search semantics. | Golden `[MS-OXOSRCH]` blob parse/serialize tests, Common Views FAI sync tests, reopen tests, and real Outlook traces proving stored blobs remain stable. | Yes only if Outlook profile bootstrap or hierarchy sync requires SFInfo rows; otherwise parity debt. |
| Arbitrary restriction tree support | `RopSetSearchCriteria` accepts only bounded `RES_AND` combinations and supported leaves such as scope, read/unread, flagged, attachment presence, categories, sender text, subject/body text, and received-date bounds. Unsupported operators, comments, subobjects, count restrictions, malformed trailing bytes, and arbitrary restrictions are rejected. | Outlook advanced Search Folder definitions can contain richer MAPI restriction trees. Accepting unsupported trees without an evaluator would create broken or misleading results. | A canonical restriction AST, evaluator, serializer, and Bcc-safe predicate policy that can prove each accepted restriction has equivalent canonical semantics. | Restriction parser golden tests, evaluator tests across mailbox rows, round-trip tests through `RopGetSearchCriteria`, unsupported-shape tests, and Bcc exclusion tests. | Conditional. It blocks public MAPI autodiscover only if real Outlook needs an unsupported tree during profile bootstrap or basic cached-mode sync. |
| Recipient and Bcc predicates | Recipient display predicates and Bcc-related predicates remain rejected. Bcc must not leak through search, AI-facing indexing, or protocol shortcuts. | Outlook search UI can express recipient-oriented searches. Incorrect Bcc handling would violate protected metadata rules. | Canonical participant indexing that separates visible recipients from protected Bcc, plus explicit rules for owner-only Bcc matching if ever allowed. | Cross-protocol search tests proving visible recipient matching works where supported and Bcc is never exposed to non-owner projections, MAPI criteria rejection tests, and AI/search indexing checks. | No for basic profile publication; yes for a claim of full Outlook search parity. |
| Common Views arbitrary view-designer parity | LPE exposes bounded Common Views named-view rows for supported default mail views and persists Outlook-created associated configuration rows. It does not advertise broad folder-local NamedView rows or complete view designer behavior. | Outlook depends on Common Views and folder views for navigation and cached profile reuse. Unsupported view descriptors can produce broken columns or stale client preferences. | A bounded Outlook view metadata model that names supported view descriptors, column packets, sort/group/collapse state, persistence scope, and object-family applicability. | MS-OXOCFG golden tests, named-view table/open tests, default-view EntryID tests, unsupported folder-local view tests, and real Outlook profile reopen evidence. | Yes if missing view rows break bootstrap or folder open; otherwise broader view parity debt. |
| Navigation shortcut breadth | `mapi_navigation_shortcuts` stores Common Views shortcut/group-header rows for bounded Outlook navigation-pane compatibility. Default Favorites rows are projected for fresh profiles, but shared-folder shortcut semantics, public-folder shortcut flags, read-only group-type extensions, and complete navigation-pane presentation parity remain deferred. | Outlook uses navigation shortcuts and WunderBar rows to populate Favorites and navigation panes across cached-mode reopen. | A canonical compatibility model for every supported shortcut type, target family, group type, flags, ordering, lifecycle, and permission behavior. | Shortcut create/update/delete/open tests, Common Views associated sync tests, shared/public-folder shortcut tests if widened, and Outlook navigation-pane traces. | Usually no for private mailbox publication unless Outlook bootstrap requires a missing shortcut shape. |
| Reminder Search Folder secondary promotion | Reminder projection is computed from canonical calendar/task/message reminder fields. Secondary sender/recipient reminder promotion and full Exchange reminder search behavior remain deferred. | Outlook reminders and To-Do surfaces depend on search-like projections over eligible reminder-bearing objects. Incomplete promotion can hide or duplicate reminders. | Canonical reminder query semantics covering every supported object family, recurrence occurrence expansion, dismissal state, folder exclusions, and any sender/recipient promotion rule that maps safely to LPE state. | Reminder search-folder content tests, recurrence/dismissal tests, folder-exclusion tests, EWS reminder tests, and Outlook reminder/To-Do evidence. | No for basic MAPI profile publication unless cached-mode tests show broken reminder bootstrap. |
| Durable categorized/collapse view state | Categorized contents table metadata and collapse state are session-local table-handle state. LPE does not persist categorized collapse state as profile data. | Outlook may preserve view expansion/collapse preferences across sessions. Persisting unsupported state incorrectly would turn client UI state into mailbox truth. | A bounded profile metadata model for view state, if traces prove server persistence is required, with clear distinction from canonical mailbox content. | Categorized table tests, collapse-state round-trip tests, reconnect/reopen tests, and Outlook evidence that server-side persistence is required. | No for profile publication unless Outlook requires durable collapse state during the gate. |

## Boundary Decision

The current Search Folder and Common Views boundary is correct for the
architecture: canonical `search_folders` and reminder-bearing objects own the
user-visible search/reminder facts, while Common Views FAI messages,
navigation shortcuts, named views, and search-definition blobs are durable
Outlook compatibility metadata only when explicitly modeled.

The remaining unsupported Exchange behavior should stay rejected until LPE can
prove an equivalent canonical evaluator, serializer, and privacy policy. In
particular, Bcc-related predicates must remain rejected or owner-safe by design;
they must never be enabled as a side effect of accepting arbitrary restriction
trees.

## Verification Expectations

Before closing any Search Folder or Common Views parity gap:

- update the architecture matrix or roadmap row that names the gap;
- add MAPI parser/serializer golden tests for accepted wire shapes;
- add canonical evaluator tests for any new active search semantics;
- add Bcc-protection tests for every recipient or participant predicate;
- add Common Views FAI sync/open/reopen tests for any new durable metadata;
- attach real Outlook evidence for any behavior claimed as required for public
  MAPI autodiscover.

## Verification Performed

Commands used for this documentation follow-up:

- `rg -n "search[- ]folder|Search Folder|Common Views|CommonViews|RopSetSearchCriteria|RopGetSearchCriteria|IPM\\.Microsoft\\.WunderBar|SearchRestriction|FolderList2|PidTagDefaultViewEntryId|default view|reminder|NamedView|WLink" docs/architecture docs/audits crates/lpe-exchange/src -g "*.md" -g "*.rs"`
