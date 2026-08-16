# MAPI over HTTP Implementation Plan

This document is the implementation contract for the guarded MAPI over HTTP
surface. It is documentation-only: changing it does not enable client
publication, widen runtime protocol support, or replace the canonical LPE data
model.

MAPI over HTTP is the first Outlook desktop Exchange-account path for Outlook
2016 and Outlook 2019 cached mode. Outlook Anywhere / RPC over HTTP remains a
later legacy compatibility shim for top-level `EXPR` autodiscover publication
and must stay aligned with real `/rpc/rpcproxy.dll` mailbox transport behavior
before it is advertised.

## Wire-Contract Requirements

### Publication and Protocol Scope

- MAPI over HTTP endpoints remain authenticated and opt-in. Autodiscover may
  publish the MAPI endpoint only when `LPE_AUTOCONFIG_MAPI_ENABLED` is enabled
  and the client capability negotiation succeeds. Recorded MAPI/HTTP evidence
  is a release-quality requirement, while
  `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` remains reserved for legacy
  `EXPR`/RPC over HTTP publication and does not control MAPI/HTTP publication.
- MAPI over HTTP startup and publication require both the exact
  `0.5.2-sql` schema label and every required physical MAPI table and
  column. A tagged but incomplete schema is a fatal storage-startup and
  readiness failure; LPE must reject it before accepting or advertising a MAPI
  session rather than exposing the database failure through an Outlook
  `Execute` request.
- The required physical shape includes `mapi_special_folder_aliases` with its
  account scope, bounded FID, 22-byte SourceKey, and separately allocated server
  CN checks, alias/SourceKey/CN uniqueness, account foreign key, and deliberately
  non-unique canonical FID.
  The last rule permits different Outlook profiles or OST replicas to retain
  different client FIDs for the same canonical special folder.
- The required shape also includes the one-row `mapi_store_identity` singleton:
  it is created with a new random database REPLGUID, owns the global GLOBCNT
  allocator, and enforces globally unique persisted object counters, object
  IDs, SourceKeys, and server change numbers. Logical default-folder roles
  remain internal and are converted to separately allocated durable identities
  at each authenticated MAPI request boundary. This follows [MS-OXCSTOR]
  section 3.2.3 and [MS-OXCFXICS] section 3.1.5.3.
- The canonical SQL schema for those checks is always `public`, independent of
  the connection `search_path`; installation pins schema creation to `public`
  and refuses relations outside it, while `Storage::connect` pins every pooled
  connection to `public`, so a user-named shadow schema cannot become parallel
  protocol state.
- Top-level `EXPR` metadata is permitted only for the later Outlook Anywhere /
  RPC over HTTP path. It must not be used to imply that MAPI over HTTP or RPC
  proxy behavior is complete before the corresponding transport is implemented.
- The first supported lab target is a single LPE server with sticky MAPI session
  state. Cross-process session replay, session migration, and load-balanced
  failover are production-hardening work.

### Transport and Session Framing

- EMSMDB `Connect`, `Execute`, and `Disconnect` traffic must maintain a
  server-side MAPI session context with strict `MapiContext` and `MapiSequence`
  handling.
- EMSMDB `Connect` and NSPI `Bind` perform full credential verification. A
  `mail-auth.mapi.login-succeeded` audit event is written only after that
  request actually establishes or reconnects its Session Context. Subsequent
  requests compare the transport/endpoint/account-bound authentication context required
  by `[MS-OXCMAPIHTTP]` section 3.2.5.1, including a process-keyed credential
  proof and the current active password or app-password verifier, without
  repeating Argon2 verification or successful-login audit writes. Session
  state retains the complete principal projection and keyed digests, never a
  plaintext credential or stored password hash. Changed, disabled, or revoked
  credentials fail the request and retain failed-authentication auditing.
  Direct MAPI/HTTP Session Contexts and legacy RPC context handles are not
  interchangeable.
- `Execute` refreshes `MapiContext` and `MapiSequence` on every accepted request.
- Required MAPI HTTP request headers, request identifiers, client information,
  cookies, request-body framing, and response-code mapping are part of the wire
  contract. Missing, malformed, stale, duplicate, replayed, or overlapping
  same-session requests must receive deterministic protocol responses.
- `X-RequestId` and `X-ClientInfo` are echoed according to the accepted request.
  Duplicate replay of the same request id and body is idempotent for transport
  purposes; reuse of the same request id with a different body is rejected.
- `Content-Length` handling must be explicit. The server must not silently
  reinterpret malformed framing, and response framing must be compatible with
  Outlook's MAPI HTTP parser.
- Stale `Disconnect` cookies and missing or malformed session cookies must fail
  at the transport/session layer without mutating mailbox state.
- Failure mapping preserves the protocol layer. Authentication, redirection,
  and exceptional HTTP failures use their HTTP status; ordinary MAPI/HTTP
  transport failures use HTTP 200 and the exact `X-ResponseCode` from
  `[MS-OXCMAPIHTTP]` sections 2.2.2.2 and 2.2.3.3.3. In particular, code 4
  means only `Invalid Header`, code 12 means `Invalid Request Body`, and an
  unexpected internal request-processing failure uses code 1 `Unknown
  Failure`. A nonzero `X-ResponseCode` uses `Content-Type: text/html` and a
  diagnostic body per section 2.2.3.2.2. A defined Execute-method failure
  instead keeps `X-ResponseCode: 0` and uses the binary failure body from
  section 2.2.4.2.3; individual ROP failures remain in their ROP
  `ReturnValue`.
- Every response associated with a resolved Session Context, including an
  `Execute` failure, returns the complete associated `Set-Cookie` set per
  `[MS-OXCMAPIHTTP]` sections 2.2.3.2.3, 3.1.1, 3.1.5.2, and 3.2.5.2. For
  EMSMDB this includes both `MapiContext` and `MapiSequence`; a request whose
  context cannot be resolved has no associated cookie set to return.

### ROP Dispatch

- ROP dispatch is terminal within the current ROP request buffer when the first
  unsupported, reserved, or malformed ROP is encountered. The server returns one
  parseable unsupported/error response for that ROP and does not execute later
  ROP bytes in the same buffer.
- Typed protocol enum boundaries are strict. Unknown `RopId`, MAPI property
  type, restriction type, sync type, FastTransfer marker, or transfer marker must
  be logged with the raw numeric value and handled through the parseable
  unsupported/error path. Unknown values must not be coerced, must not panic, and
  must not produce partial side effects.
- Private mailbox logon is the primary mailbox logon mode. Public-folder
  logons are supported only for the bounded canonical public-folder projection
  documented below; unmodeled public-folder behavior returns parseable protocol
  errors and must not create protocol-local public-folder state.
- `RopOpenFolder` validates and opens the requested Folder object, then returns
  only its documented handle/status/rules/replica response fields. It must not
  pre-project a broad folder-property map; later property ROPs perform the
  requested projection. This keeps Inbox opening independent of unrelated
  property work, as specified by `[MS-OXCROPS]` section 2.2.4.1.2 and
  `[MS-OXCFOLD]` section 3.2.5.1.
- ROP folder and message identifiers use the MAPI wire layout at the protocol
  boundary: two-byte little-endian `REPLID` followed by a six-byte big-endian
  `GLOBCNT`. LPE's internal store id remains `GLOBCNT << 16 | REPLID`, and
  conversion between the two layouts must happen only when parsing or
  serializing ROP request and response fields, table identifier columns,
  FastTransfer/ICS identifier properties, and identifier-valued property rows
  such as `PidTagFolderId`, `PidTagParentFolderId`, and `PidTagMid`.
- `RopLongTermIdFromId` also accepts Outlook's observed conversion request
  layout with six-byte `GLOBCNT` followed by two-byte little-endian `REPLID`;
  responses still use the canonical LongTermID form with the store replica GUID.
- `RopLongTermIdFromId` also accepts stale `REPLID` values when the embedded
  `GLOBCNT` maps to an LPE-advertised MAPI special folder, including the
  observed stale short-ID forms that carry the six-byte counter before or after
  the `REPLID` and in either counter byte order; normal mailbox items still
  require the canonical store replica id.
- `RopLongTermIdFromId` also accepts Outlook's observed bare little-endian
  six-byte counter form for advertised IPM subtree virtual folders and dynamic
  content objects already emitted by FastTransfer/ICS, such as `Conflicts`,
  Common Views FAI/search objects, and message change counters; zero and
  private-logon-only reserved counters still fail with `ecNotFound`.
- `RopLongTermIdFromId` failures for unmapped or unusable `REPLID` values use
  the documented `ecNotFound` result, not a generic invalid-parameter result.
- `RopLongTermIdFromId` and `RopIdFromLongTermId` require the exact live
  private-mailbox or public-folder Logon object. A Folder, Message, table, or
  other live object cannot borrow replica-mapping authority through
  containing-folder lineage and returns `ecNotSupported`. The operations are
  Logon ROPs under `[MS-OXCROPS]` sections 2.2.3.8 and 2.2.3.9, with the
  conversion semantics in `[MS-OXCSTOR]` sections 2.2.1.8 and 2.2.1.9;
  `ecNotSupported` is LPE's explicit wrong-live-object policy.

### EMSMDB, NSPI, and FastTransfer

- EMSMDB behavior must stay bounded to the Outlook cached-mode bootstrap,
  hierarchy, content synchronization, table, property, submission, and mutation
  surfaces explicitly covered by this plan.
- NSPI behavior is address-book resolution over canonical LPE account and
  contact visibility. NSPI mutation and link-table write behavior remain
  deferred.
- FastTransfer and ICS payloads use the MS-OXCFXICS wire grammar, including
  lexical value sizes such as two-byte `PtypBoolean` values. A full normal or
  FAI `messageChange` ends when the following grammar marker begins; LPE must
  not insert a null property tag as an object terminator.
- The clean-profile `202608111013` Calendar trace confirms that the repaired
  NSPI row, imported LastModificationTime handling, access values, appointment
  classification, and recipient collection are all active. Outlook accepts
  the web-update CN, but local upload materialization of the subsequent edit
  still fails with `MAPI_E_NOT_FOUND` before
  `RopSynchronizationImportMessageChange`. The next trace-backed
  message-identity divergence is in the full content-sync property list: LPE
  injects root EntryID, ParentEntryID, and table InstanceKey values, while
  Exchange 2016 reference captures with the same sync flags keep those provider-local values
  out of the root message properties. Normal and special-message ICS now
  follow that Exchange shape; recipient-row identities remain unchanged. This
  follows `[MS-OXCFXICS]` sections 2.2.4.3.13, 2.2.4.3.14, 3.2.5.10,
  3.2.5.12, and 4.5 and `[MS-OXPROPS]` sections 1.3.3 and 2.744. The following
  clean-profile rerun shows that correction was present but did not eliminate
  the Outlook error.
- The clean-profile `202608111212` rerun confirms that those root identity
  properties are absent on the wire, but Outlook still records the Calendar
  download as `MAPI_E_NOT_FOUND`. The first Calendar failure is already the
  state-only download immediately after the successful initial Outlook create,
  before the web edit. LPE serialized a contents final state as
  `CnsetSeen, IdsetGiven, CnsetSeenFAI, CnsetRead`; two Exchange 2016 content
  downloads and the `[MS-OXCFXICS]` section 4.5 example serialize
  `CnsetSeen, CnsetSeenFAI, IdsetGiven, CnsetRead`. All LPE content-download
  state writers now use that Exchange order, including the client-state-selected
  delta path exercised by Outlook. Hierarchy state remains
  `CnsetSeen, IdsetGiven`. Ordinary upload state remains
  `CnsetSeen, CnsetSeenFAI, CnsetRead` and never contains `IdsetGiven`.
- The later `202608111553` rerun in the Probe-B Outlook profile confirms that
  the Exchange-order state correction is active on both the initial and
  web-update Calendar downloads, but it does not eliminate the Outlook error.
  Across the Probe B, Probe C, and Probe D traces, Outlook supplies a distinct
  16-byte `PidTagSearchKey` on the initial appointment Save and LPE returns a successful
  `RopSetProperties` result. LPE then discarded that value, suppressed the
  creator's own uploaded item from the immediate download, and substituted the
  canonical event UUID as SearchKey on the first later full download. The
  SourceKey, GlobalObjectId, and CleanGlobalObjectId remain stable while only
  SearchKey changes; Outlook subsequently reports `MAPI_E_NOT_FOUND` before it
  can submit the locally edited item.
- Calendar import now applies the same bounded compatibility policy already
  used for imported configuration FAI SearchKeys: a valid 16-byte binary value
  accepted before the first Save is stored durably, survives canonical web
  updates and reconnects, and is immutable afterward. Repeated SearchKey values
  on an existing ICS import are silently disregarded, while direct later Set or
  Delete operations retain the normal read-only property problem. Web-created
  events without an imported value continue to use the canonical UUID fallback.
  The implementation never derives SearchKey from GlobalObjectId data because
  those properties have independent semantics. `[MS-OXCPRPT]` section 2.2.1.9
  defines SearchKey stability, and `[MS-OXCMSG]` section 2.2 product note `<1>`
  records Exchange's exception for this otherwise read-only property. LPE
  deliberately bounds that compatibility to the pre-first-Save value observed
  in the Probe B/C/D traces. Their correlation makes this the next evidence-backed
  Outlook compatibility experiment, not a proven general Exchange appointment
  contract; a fresh-profile real-client rerun is required to establish symptom
  elimination.
- The later `202608111835` Probe E run confirms that durable SearchKey
  preservation, root provider-identity omission, and Exchange-order download
  state are all active, but the Calendar edit still fails locally before
  `RopSynchronizationImportMessageChange`. It also exposes the same exact
  earlier server failure present in Probe B, C, and D: Outlook imports the
  targetless Calendar Common Views header `Team: test` with raw
  `PidTagWlinkType = 5`, and LPE rejects its Save with `MAPI_E_NOT_FOUND`.
  LPE now preserves type `5` only for that complete Calendar group-header
  shape; `[MS-OXOCFG]` section 2.2.9.5 still defines type `4`, so this is an
  Outlook 16.0.20228 product variant rather than a new protocol enum value.
  Common Views and Calendar use distinct collectors and the later appointment
  import succeeds, so eliminating the cross-folder symptom remains a
  real-client rerun question even though the WLink rejection itself is a
  definite server defect.
- The same Probe E capture contains six Outlook-created `Synchronization Log:`
  Messages moved to Deleted Items. Their body-property upload uses
  `PidTagRtfCompressed` without `PidTagBody` or `PidTagHtml`; LPE previously
  reduced each canonical Message to its headers and an empty body, hiding the
  diagnostic text from the web interface and mailbox APIs. LPE now validates
  the complete MS-OXRTFCP container, CRC, declared size, and LZFu references,
  then uses bounded Windows-1252/Unicode RTF text extraction as the canonical plain-body
  fallback. The exact first Probe E value round-trips through
  `RopCreateMessage`, `RopSetProperties`, and `RopSaveChangesMessage` into a
  readable body containing `Error synchronizing view/form` and
  `[8004010F-501-8004010F-320]`. This deliberately does not preserve rich-RTF
  formatting or claim arbitrary rich-text round-trip fidelity.
- The `202608112137` Probe F rerun confirms that the Common Views type-5 fix,
  imported Calendar SearchKey preservation, provider-identity omission,
  Exchange-order content state, the then-enabled server-computed upload
  `IdsetGiven`, and RTF
  diagnostic ingestion are active. Outlook still logs Calendar folder
  `[8004010F-501-0-1430]` immediately after the successful create download and
  after applying the later web update, then rejects the final local edit with
  `[8004010F-501-0-0]` before sending an item import ROP. The readable Trash
  reports exposed a mailbox identity contradiction: Outlook successfully
  imports and retains its Junk default-folder alias in hierarchy
  `MetaTagIdsetGiven` and continues using that EntryID, while LPE previously
  returned `MetaTagIdsetDeleted` for the same alias even though it kept the
  durable redirect valid. A successful hierarchy import now always advances
  `MetaTagCnsetSeen`, and the alias remains resident in the originating OST's
  `MetaTagIdsetGiven` without being emitted as a second hierarchy row, following
  `[MS-OXCFXICS]` sections 3.2.5.9.4.3 and 3.3.5.8.8. The same trace proves the
  root `Depth` hierarchy omitted the persisted Conflicts, Local Failures, and
  Server Failures children even though the Inbox advertised their EntryIDs;
  Outlook's attempt to move the failed appointment to Local Failures therefore
  could not resolve that OST folder. Root-depth and direct Sync Issues tables
  now expose those canonical child mailboxes and mark Sync Issues as having
  subfolders. The exact appointment create-versus-web-download comparison also
  found two server projection
  defects: `PidTagMessageDeliveryTime` was replaced by the appointment start
  instead of the server-receipt time, and the first start/end-display
  `TZRULE.wYear` was changed from Outlook's `0x0641` to zero. Saved Calendar
  projection now uses the durable creation time for delivery time and emits the
  documented `0x0641` first-rule year. Probe E and F also share the same first
  post-Save hierarchy-table divergence: the appointment-caused Calendar row
  refresh was collapsed to folder-only flags `0x0100`. LPE now retains the
  `[MS-OXCNOTIF]` message-cause `M` bit and its required zero row-key fields,
  producing `0x8100` for ordinary item changes while NewMail remains `0xC100`.
  These are protocol/state corrections; a new-profile real-client rerun is
  still required to establish which one was causal for Outlook's local
  `MAPI_E_NOT_FOUND`.
- The clean-database, clean-profile `202608121206` Probe G rerun falsifies the
  Probe F hierarchy and special-folder-alias corrections as sufficient causes.
  Outlook opens and content-synchronizes Sync Issues and Local Failures, and
  its `PidTagAdditionalRenEntryIds` writes repeat the canonical first four
  Sync-Issues-family EntryIDs; only the Junk EntryID is a client alias. The
  first readable Trash report instead records that the new appointment was
  added to the online Calendar and then immediately reports folder
  `[8004010F-501-0-1430]`. The zero-change content download in that interval is
  Exchange-shaped and state-only. The preceding upload request exposes the
  concrete contradiction: after importing and saving the appointment with a
  foreign `PidTagChangeKey`, Outlook issues `RopSaveChangesMessage` and an
  immediate `RopGetPropertiesSpecific` for only `PidTagChangeKey` against the
  same aliased input/response handle. LPE correctly committed the imported
  SourceKey/ChangeKey beside a distinct server CN, but the Event path treated
  the request's unrecognized `0x08` SaveFlags bit as an effective no-keep-open
  save and removed the Message
  object before the later ROP in the same request buffer. Property projection
  then treated the missing object as the Root folder and returned the Root
  ChangeKey instead of the appointment's committed imported ChangeKey.

  LPE now applies a bounded compatibility policy to successful Event and
  Contact saves with no recognized keep-open flag: retain the committed Message
  object read-only until the remaining ROPs in that Execute buffer have run,
  then close it and clear all per-handle state. This is an LPE end-of-Execute
  lifetime rule, not a protocol claim that every no-keep-open save MUST close at
  that boundary. It lets Probe G's post-save read return the committed imported
  ChangeKey without leaving the Message open across requests. If
  the input and response indexes alias, containing-Folder projection is deferred
  until the read has run; a distinct response index is projected immediately.
  Explicit keep-open flags and other message-object types retain their existing
  behavior. `[MS-OXCROPS]` section 3.2.5.1 requires in-order ROP processing,
  `[MS-OXCMSG]` sections 2.2.3.3.1 and 3.2.5.3 define SaveFlags and successful
  Message-save processing, and `[MS-OXOCNTC]` sections 3.1.1, 3.1.4.1.1, and
  3.1.4.1.3 define Contact objects as Message extensions saved through that
  Message path. `[MS-OXCFXICS]` section 3.3.4.3.3.2.2.2 separately documents a
  post-save `RopGetPropertiesSpecific` pattern for new PCL and change-number
  values; Probe G's actual follow-up request was only for `PidTagChangeKey`.

  For every successful non-embedded `RopSaveChangesMessage`, the response
  handle index identifies the containing Folder even when an earlier
  `RopRelease` removed the original parent handle. LPE reuses a live handle for
  that exact Folder when one exists; otherwise it allocates a fresh Folder
  handle and writes it into the response slot. It does not resurrect the
  released numeric handle or substitute the saved Message or a different
  Folder. `[MS-OXCMSG]` section 3.2.5.3 defines the containing-Folder response
  object, and `[MS-OXCROPS]` sections 2.2.6.3.1 and 2.2.6.3.2 define the two
  handle indexes and success response. Reuse versus allocation is LPE's
  handle-table implementation policy, not a protocol-mandated allocation
  algorithm.

  The Root fallback was not confined to Probe G. Direct
  `RopGetPropertiesSpecific`, `RopGetPropertiesAll`, and
  `RopGetPropertiesList` now require a live property-bearing Logon, Folder,
  Message, or Attachment handle. Live table, stream, notification,
  synchronization, and FastTransfer control objects return `ecNotSupported`
  instead of projecting Root or containing-Folder properties.
  `PublicFolderLogon` remains a Logon property object rather than being
  projected as Root, and FAI and LPE's other message-object variants enumerate
  Message tags rather than Folder tags. These object-type boundaries follow
  `[MS-OXCPRPT]` sections 1.1, 1.5, 1.6, and 3.2.5.1 through 3.2.5.3;
  `ecNotSupported` is LPE's explicit failure policy for a live handle outside
  those property-object types.

  Stream continuation ROPs require the actual live Stream server object that
  `RopOpenStream` returned; in LPE that object is the `AttachmentStream` handle
  variant for both attachment and property streams. `RopReadStream`, the write
  and write-and-commit variants, `RopCommitStream`, size and seek operations,
  region lock/unlock, and `RopCloneStream` never coerce a parent Message into
  its child stream. `RopCopyToStream` requires exact live Stream objects at
  both its source and destination handles. A different live object returns
  `ecNotSupported`. The Stream-only object contract follows `[MS-OXCPRPT]`
  sections 2.2.15 through 2.2.22 and 2.2.24 through 2.2.27; the selected error
  is LPE's explicit wrong-live-object policy.

  Folder and synchronization ROPs enforce the same object boundary before
  looking at an object's containing-folder lineage. `RopOpenFolder`,
  `RopOpenMessage`, and `RopCreateMessage` accept only a Logon or Folder input;
  Folder-only create/delete/move/copy, permissions, rules, hierarchy-table,
  synchronization-configure, source-copy-messages, and collector-open ROPs
  accept only a Folder. Upload ROPs accept only a synchronization collector of
  the required content or hierarchy type. A different live object returns
  `ecNotSupported` without binding an output handle or mutating canonical
  state, except that `[MS-OXCMSG]` section 3.2.5.1 explicitly requires
  `RopOpenMessage` to return `ecNullObject` for the wrong input-object type.
  This prevents a Message's containing Folder from accidentally
  authorizing a Folder or upload operation (`[MS-OXCFOLD]` section 2.2.1.1.1,
  `[MS-OXCMSG]` sections 2.2.3.1 and 2.2.3.2, and `[MS-OXCFXICS]` sections
  2.2.3.2.1.1.1 and 2.2.3.2.4.1.1).

  Input-handle validation is also centralized across dispatched ROPs. A handle
  value never assigned to an open object returns `ecNullObject`; a previously
  issued handle whose object has been released or closed and not recycled
  returns `ecInvalidObject`, including a later ROP in the same buffer after
  `RopRelease`. Server-handle allocation no longer advances from untrusted
  client handle-table values and never assigns reserved `0xFFFFFFFF`. The error
  distinction and Release lifecycle follow `[MS-OXCROPS]` sections 3.2.5.1,
  3.2.5.3, and 3.2.5.4; allocator independence from client numeric high-water is
  an LPE hardening rule, while the prohibition on assigning `0xFFFFFFFF` is
  specified in section 3.2.5.1.

  `RopReadPerUserInformation`, `RopWritePerUserInformation`, and
  `RopGetAddressTypes` likewise require an exact live private-mailbox or
  public-folder Logon object. They do not accept a Folder, table, Message, or
  other live object merely because it belongs to the same store. The per-user
  ROPs are explicitly issued against private-mailbox or public-folder logons in
  `[MS-OXCSTOR]` sections 2.2.1.12 and 2.2.1.13; `[MS-OXOMSG]` section 2.2.4.3
  specifies a Logon object for `RopGetAddressTypes` even though the server
  ignores that object's value. LPE returns `ecNotSupported` for a different
  live object.
- The Probe F comparison also exposed an incomplete Calendar Message-property
  replay surface after the appointment had saved successfully. That is a server
  contradiction worth correcting, but the trace does not establish that any
  one property below caused Outlook's later local `MAPI_E_NOT_FOUND`. Each
  canonical Event now has a durable, bounded passthrough bag for unmapped
  mailbox-assigned named properties and only these standard properties:
  `PidTagAlternateRecipientAllowed`, `PidTagImportance`,
  `PidTagOriginatorDeliveryReportRequested`, `PidTagPriority`,
  `PidTagReadReceiptRequested`, `PidTagSensitivity`,
  `PidTagResponseRequested`, `PidTagConversationTopic`,
  `PidTagConversationIndex`, `PidTagReplyRequested`,
  `PidTagDeleteAfterSubmit`, `PidTagConversationIndexTracking`,
  `PidTagInternetCodepage`, and `PidTagMessageLocaleId`. Set and delete changes
  remain handle-local until the parent Message Save commits them atomically;
  after Save they survive reconnect. Calendar `RopGetPropertiesAll` and
  `RopGetPropertiesList` enumerate the canonical property set plus the effective
  persisted and staged passthrough set, so a same-handle Set is visible and a
  same-handle Delete is absent. A canonical property projection always replaces
  any stale stored value with the same property ID, including in ICS, and the bag
  cannot become parallel subject, body, organizer, attendee, identity, version,
  attachment, or reminder state. This follows `[MS-OXCPRPT]` sections 2.2.3,
  2.2.4, and 3.2.5.2 through 3.2.5.5.

  `PidLidAppointmentColor` remains client-authored compatibility state because
  the canonical Event model has no color field. LPE validates the defined
  values `0` through `10` and preserves the selected value across Save, web
  updates, reconnect, direct reads, and full ICS rather than acknowledging it
  and replacing it with zero (`[MS-OXOCAL]` section 2.2.1.50).

  Probe H (`202608122016`) exposed a generic typed-value corruption in that
  passthrough path: Outlook imported `PidLidIntendedBusyStatus`
  (`0x82240003`) as `-1`, but the common MAPI encoder attempted an unsigned
  conversion and silently persisted zero. The first state-only Calendar replay
  already returned zero, before the later web edit. LPE now writes signed
  `PtypInteger16`, `PtypInteger32`, and `PtypInteger64` values with their exact
  two's-complement wire bits; unsigned representations and the separate
  FolderId, ParentFolderId, MessageId, and ChangeNumber object-ID encodings keep
  their existing semantics. The imported value must survive Save, snapshot
  reload, a canonical Event update, and a fresh full Calendar ICS projection.
  This follows `[MS-OXCDATA]` section 2.11.1, `[MS-OXPROPS]` section 2.151,
  and `[MS-OXOCAL]` section 2.2.6.4. A complete comparison of the Probe H
  upload and returned appointment found no other unexplained property loss;
  a fresh Outlook run is still required to establish whether preserving this
  value removes the remaining client-side `MAPI_E_NOT_FOUND` sync report.

  Probe I (`202608130758`) confirms that the signed-value fix is active, but
  the Calendar errors persist. Its first remaining server contradiction occurs
  immediately after Outlook imports and saves the new appointment: upload
  collector request `:145` returns `MetaTagIdsetGiven` containing the newly
  assigned MID, and Outlook reports the item added online followed by
  `MAPI_E_NOT_FOUND` in the same second. `[MS-OXCFXICS]` sections 3.1.5.2.1 and
  3.2.5.2.1 explicitly require an upload context to ignore this state property and not
  return it through `RopSynchronizationGetTransferState`. The matching normal
  content-import LPE interoperability capture (`logs/202608041041.saz` raw 216,
  221, and 223)
  returns `CnsetSeen`, `CnsetSeenFAI`, and `CnsetRead`, but no `IdsetGiven`.
  An earlier capture used to justify LPE's exception instead followed
  `RopSynchronizationImportMessageMove` operations and was not a valid normal
  `ImportMessageChange` comparator. LPE therefore no longer stages imported
  SourceKeys for upload-state generation: successful imports advance only the
  applicable server CN sets, while the client updates its local `IdsetGiven`
  after a successful import as described by section 3.3.5.8.7. This removes a
  definite protocol violation at the first failing boundary; a fresh Outlook
  run remains required to prove end-to-end symptom elimination.

  Probe J (`202608130853`) confirms that upload GetTransferState now omits
  `MetaTagIdsetGiven`. Calendar import `:132` supplied a client ChangeKey,
  Save/GetProps `:136` returned that same 20-byte client ChangeKey, and final
  state `:137` advertised a distinct server CN. That tuple is the required
  `[MS-OXCFXICS]` section 3.1.5.3 result: successful ICS import preserves the
  supplied ChangeKey/PCL while assigning a server-internal CN that can enter
  `MetaTagCnsetSeen`. The later contrary interpretation came only from the LPE
  interoperability capture `logs/202608041041.saz`; it was not Exchange
  product evidence. Exchange associated-message controls also preserve their
  imported ChangeKeys, consistent with the same protocol rule.

  Probe K (`202608131242`) confirms on a fresh database that the experimental
  Calendar server-ChangeKey replacement is active, but the three Calendar
  synchronization reports remain. Probe N (`202608140800`) still reports the
  identical create/download/local-edit Calendar failure triad, so that
  replacement is both ineffective and contrary to section 3.1.5.3; Calendar
  imports again preserve the supplied ChangeKey/PCL beside their distinct
  server CN. The first post-Save Outlook activity repeatedly opens and
  reads `LocalFreebusy`. That run exposed a separate durable-object defect:
  LPE advertised one constant high MID with an epoch modification time and a
  ChangeKey derived from the synthetic MID. The genuine Exchange 2016
  `raw/548` control instead returns absent Delegate Information cells as
  `ecNotFound`, followed by a durable LMT, an independently versioned
  server ChangeKey, a 46-byte provider EntryID, and the message class.
  `LocalFreebusy` is therefore now a normal-content MAPI projection with one
  account-scoped `mapi_object_identities` row. Its MID and SourceKey remain
  stable; changes to canonical default-Inbox/default-Calendar delegation,
  account-wide sender rights, `delegate_preferences`, or a projected delegate's
  name/email rotate its CN, ChangeKey, PCL, and LMT. The canonical tuple read
  and identity rotation share one revision-fenced transaction so a concurrent
  write cannot consume a revision with stale property content. Ordinary
  Calendar item changes do not rotate it.
  A deeper Probe N (`202608140800`) ETL and EMSMDB image audit finally
  localizes the unchanged Calendar `[8004010F-501-0-1430]` report: duplicate
  processing asks Outlook's cached special-folder state for the Conflicts
  folder EntryID, but that cache is empty because the Inbox
  `PidTagAdditionalRenEntryIds` value never completed a hierarchy round trip.
  The wire and PostgreSQL state show both halves of the LPE defect. LPE accepted
  Inbox hierarchy versions whose property bag contained empty reserved slots
  but discarded those hierarchy property values; later Root
  `RopSetProperties` calls persisted corrected profile bytes without rotating
  or journaling the Inbox hierarchy version. Outlook therefore uploaded the
  same empty slots again and failed before Calendar duplicate reconciliation.
  The repair belongs to Inbox hierarchy state: export the normalized
  multi-binary value in `folderChange`, and commit every accepted direct write
  or hierarchy import with its Inbox version and replay row in one transaction.
  This is independent of `LocalFreebusy` and Calendar item content.
  Probe L (`202608131831`) proves the durable identity and normal-content
  partition are active, but also exposes a missing table-row boundary: Outlook
  asks the FreeBusy Data contents table for `PidTagFolderId`, `PidTagMid`,
  `PidTagInstID`, and `PidTagInstanceNum`; all four identify the same durable
  `LocalFreebusy` row. LPE therefore returns the FreeBusy Data FID, the durable
  MID for both MID and instance ID, and instance number zero, following
  `[MS-OXCTABL]` sections 2.2.1.1 and 2.2.1.2 rather than flagging three of the
  requested cells as `ecNotFound`.
  Probe M (`202608131919`) proves that this row correction is active: Outlook
  opens the advertised durable message. It then exposes the next exact
  boundary before Calendar synchronization: an empty `ROWLIST_REPLACE` on the
  Freebusy Data folder returned `ecNotFound`, and a provider-private
  `fixupfbfolder=FALSE` named-property write on `LocalFreebusy` returned
  `ecNotSupported`. The fresh Freebusy Data table now accepts that exact empty
  replacement as a no-op. Nonempty ACL copying remains fail-closed until the
  canonical boundary can preserve account-scoped member IDs, Calendar Author
  versus Editor, Freebusy Editor, and atomic replacement without widening
  rights. The private named flag is durable custom MAPI metadata on the fixed
  object and does not replace grants or preferences: Set/Delete stays local to
  the Message handle, same-handle reads see the staged value, Release discards
  it, and successful Save atomically persists it while rotating the object's
  CN, ChangeKey, PCL, and LMT. The saved handle is replaced from the complete
  property bag reread under that transaction, so a concurrent property save
  cannot attach a stale bag to the newest ChangeKey. Fresh unconfigured Delegate Information fields
  remain absent, matching genuine Exchange `raw/548` product evidence.
  The same Probe M request `:91` registers `ObjectModified (0x0010)` against the
  exact Freebusy Data FID and durable `LocalFreebusy` MID before that write. LPE
  now preserves the optional registration MID instead of discarding it: zero is
  folder scope and nonzero matches only that FID/MID pair. A successful custom
  property Save journals and emits the message `ObjectModified` event for
  replay by other live sessions, and Freebusy Data incremental ICS consumes the
  same version; a Save with no staged custom-property mutation does not. This follows `[MS-OXCROPS]` section
  2.2.14.1.1 and `[MS-OXCNOTIF]` sections 2.2.1.1, 2.2.1.2.1.1, and 3.1.5.1.
  The projection has no parallel content table and its private identity is not
  exposed through REST or JMAP. REST mailbox delegation and JMAP mailbox
  `Share` instead read and patch the same canonical `delegate_preferences`
  tuple that EWS already uses. A fresh Outlook run is still required to prove
  whether this is the remaining Calendar interoperability boundary.

  For the current non-delegated Event model, the canonical organizer supplies
  the complete `PidTagSender*` and `PidTagSentRepresenting*` identity families;
  LPE does not invent a distinct delegate sender without canonical delegate
  state. This is the same-identity case in `[MS-OXOMSG]` sections 2.2.1.48
  through 2.2.1.59; `[MS-OXCICAL]` section 2.1.3.1.1.20.61 defines the distinct
  `X-MS-OLK-SENDER` delegate case. Canonical body state supplies
  `PidTagNativeBody` as undefined (`0`), plain text (`1`), or HTML (`3`) and keeps
  `PidTagRtfInSync=FALSE` because LPE does not project a synchronized RTF body,
  following `[MS-OXCMSG]` sections 2.2.1.58.2 and 2.2.1.58.5.
  `PidTagIconIndex` is derived as single/recurring appointment or
  single/recurring meeting (`0x0400` through `0x0403`) per `[MS-OXOCAL]`
  section 2.2.1.49. Finally, an effective Calendar property set is rejected
  unless `PidTagResponseRequested` and `PidTagReplyRequested` agree; this
  preserves the RSVP invariant in `[MS-OXCICAL]` section 2.1.3.1.1.20.2.5,
  `[MS-OXOCAL]` sections 2.2.1.36-2.2.1.37, and `[MS-OXOMSG]` section 2.2.1.45.
- Durable Calendar custom-property validation, immutable SearchKey handling,
  and storage lookup are isolated in `mapi_events/custom_properties.rs`. This is
  the first split boundary for the thousand-line `mapi_events.rs`; identity
  allocation remains in `mapi_events/imported_identity.rs`, and a later split
  should move attachment/reminder persistence rather than adding unrelated
  behavior back to the hub.
- `mapi_store/snapshot.rs` remains a legacy oversized aggregation point. Its
  next split boundary is the fallback/synthetic-object constructors and their
  identity helpers; move those into `mapi_store/snapshot/fallback.rs` before
  adding further snapshot behavior. This SearchKey change only supplies the new
  version field in the existing fallback constructor.
- `mapi/properties/streams.rs` crossed the thousand-line review threshold.
  Calendar-specific open/read overlay, ownership validation, and staged stream
  mutation now live in `mapi/properties/streams/calendar.rs`, keeping the parent
  below the hard production limit. Its next split should move the remaining
  generic object-property open/read construction into `streams/open.rs`, leaving
  writable-target, seek, copy, commit, and non-Calendar mutation helpers in the
  parent.
- `mapi_store.rs`, `mapi/store_adapter.rs`, and `store.rs` remain touched hubs
  above the thousand-line threshold. Move Calendar snapshot loading and wiring
  into the existing `mapi_store/snapshot/calendar.rs`, Calendar Event projection
  and hydration into a focused `mapi/store_adapter/calendar.rs` helper, and the
  Calendar custom-property trait DTOs and methods into
  `store/calendar_properties.rs`, and hierarchy version/profile DTOs and
  methods into `store/hierarchy.rs`; leave the parent files as wiring surfaces.
- `mapi/identity.rs` has reached the thousand-line split threshold. Before
  extending identity behavior again, move the task-local request identity
  scope and `MapiIdentityCodec`, including durable/logical alias encoding and
  decoding, into `mapi/identity/scoped.rs`; leave reserved special-folder
  constants, raw wire-format helpers, and public compatibility wrappers in the
  parent module. Preserve request scoping and verify the split with the focused
  identity tests and PostgreSQL WLink round-trip.
- For an extended `Execute` request with `Chain` set and a terminal
  `RopFastTransferSourceGetBuffer`, LPE returns the original response followed
  by independently framed synthetic GetBuffer responses until the transfer is
  complete or the documented response limits apply. Every frame repeats the
  response handle table and only the final `RPC_HEADER_EXT` has `Last`. LPE
  advertises `ULTRA_LARGE_PACKED_DOWNLOAD_BUFFERS` only with this behavior.
  This follows `[MS-OXCRPC]` sections 2.2.2.2.19 and 3.1.4.2.1.2.2.
- `RopFastTransferSourceCopyTo` and `RopFastTransferSourceCopyProperties` on a
  Message object return `messageContent` directly. `StartMessage` and
  `StartFAIMsg` are `message`-element wrappers and must not surround that root.
  This follows `[MS-OXCFXICS]` sections 2.2.3.1.1.1, 2.2.3.1.1.2, 2.2.4.2,
  2.2.4.3.16, and 2.2.4.4. The separate Folder-object `folderContent` root
  remains outside the currently validated CopyTo/CopyProperties surface.
  For Message-object recipient and attachment subobjects, LPE applies `Level`,
  the CopyTo exclusion list, and the CopyProperties inclusion list. An included
  collection is preceded by `MetaTagFXDelProp` even when it is empty, while an
  excluded collection has no directive. Direct `messageContent` downloads omit
  the provider-internal `PidTagAssociated` and `PidTagMid`; FAI status remains
  represented by the server-owned `PidTagMessageFlags.mfFAI` bit. Root-property
  selection applies to every emitted Message property, including normal
  canonical, generated MAPI, and persisted named properties; it is not limited
  to a special-message subset. An empty CopyTo exclusion list retains all
  eligible direct properties, while an empty CopyProperties inclusion list
  retains none. Descendant collection selection remains independent. An FAI
  without a persisted flag value falls back to `mfFAI` alone, while an effective
  value accepted at the first successful Save is replayed unchanged (for
  example, `0x00000049`, not `0x00000040`). A missing `PidTagBody` remains
  absent; only an explicitly persisted empty body is emitted as a present
  zero-length property. The server also projects exactly one read-only
  `PidTagAccess` and `PidTagAccessLevel` in direct and ICS special-message
  downloads when their property filters include them. The current owner-only
  model emits message rights `0x00000007` and access level `0`; writable handles
  and shared-folder rights will require handle-effective values rather than a
  broader constant. The direct CopyTo value `0` is an interoperability inference
  from the captured read-only `RopOpenMessage`, corroborated by the ICS example,
  rather than an explicit FastTransfer handle-to-value rule; the ICS projection
  is protocol alignment, not the proven cause of the Outlook report. This
  follows `[MS-OXCMSG]` sections 2.2.1.1, 2.2.1.6, and 2.2.3.1.1,
  `[MS-OXCPRPT]` sections 2.2.1.1 and 2.2.1.2,
  `[MS-OXCFXICS]` sections 2.2.1.7, 2.2.3.1.1.1.1, 2.2.3.1.1.2.1,
  2.2.3.2.1.1.1, 2.2.4.1.5.1, 2.2.4.3.12, 2.2.4.3.13, 2.2.4.3.16,
  3.2.5.8.1.1, 3.2.5.8.1.2, 3.2.5.9.1.1, 3.2.5.10, 3.2.5.12, and 4.5,
  `[MS-OXBBODY]` section 2.1.3.1, and `[MS-OXPROPS]` sections 1.3.3, 2.505,
  and 2.507. Direct Message-root filtering covers every property currently
  emitted by the normal and special-message serializers.
- The `202607221041` real-Outlook rerun emitted the corrected 547-byte FAI
  `CopyTo` stream but still increased the synchronization-report count from 9
  to 10 and then 11, so `PidTagAccess`/`PidTagAccessLevel` was not the sole
  cause. The next bounded hypothesis projects effective
  `PidTagHasAttachments` and `PidTagMessageStatus` through the same
  `CopyTo`/`CopyProperties` and ICS filters, including `PtypUnspecified`
  property-ID matching. `PidTagHasAttachments` stays
  coherent with `PidTagMessageFlags.mfHasAttach`; a status present in the
  canonical special-message fact is retained and this bounded projection
  otherwise defaults to zero. The zero fallback is an
  interoperability inference corroborated by `[MS-OXCFXICS]` section 4.5, not
  a requirement that every Message persist `PidTagMessageStatus`, and remains
  unproven pending a real-Outlook retest. This follows `[MS-OXCMSG]` sections
  2.2.1.2, 2.2.1.6, and 2.2.1.8, `[MS-OXCFXICS]` sections
  2.2.3.1.1.1.1, 2.2.3.1.1.2.1, 2.2.4.3.16, 3.2.5.8.1.1,
  3.2.5.8.1.2, 3.2.5.10, 3.2.5.12, and 4.5, `[MS-OXPROPS]`
  sections 2.717, 2.793, and 2.800, and `[MS-OXCDATA]` section 2.11.1.
- The `202607242304` real-Outlook rerun, after the NSPI correction, increased
  the synchronization-report count from 1 to 2 and then 3. Outlook created
  each report in Deleted Items after downloading the persisted Inbox
  `IPM.Configuration.MessageListSettings` FAI. Its direct
  `RopFastTransferSourceCopyTo` payload omitted `PidTagRecordKey`, but this is
  not a credible root cause: `[MS-OXCMSG]` section 6 product note `<3>` states
  that Exchange 2010, 2013, 2016, and 2019 do not support that property, so
  Outlook 16 must tolerate its absence. The experimental RecordKey projection
  was therefore removed rather than retained as a trace-specific workaround.
  The first confirmed divergence is the immediately following Inbox
  `PidTagLocalCommitTimeMax` (`0x670A`, `PtypTime`) read. In both
  `202607241721` and `202607242304`, LPE returned a synthetic time derived from
  the folder CN even though the just-downloaded FAI had a later canonical
  `PidTagLastModificationTime`. Outlook timestamped the synchronization report
  90 ms and 74 ms, respectively, after receiving that stale folder watermark.
  LPE now obtains the normal-message watermark from one canonical PostgreSQL
  aggregate per mailbox over `mailbox_messages.updated_at` and
  `mail_change_log.created_at` for `mailbox_message` changes. It is independent
  of both the complete full-snapshot message scope and selective snapshots that
  load no normal messages. The result is combined only with real persisted FAI,
  collaboration-object, and direct-child-folder modification times; synthetic
  FILETIME values derived from change numbers are never mixed with those real
  timestamps. The legacy change-number fallback remains only for callers that
  have no canonical folder aggregate at all. The PostgreSQL regression verifies
  import, read/flag mutation, source and destination move activity, attachment
  add/delete, and message deletion. The realistic MessageListSettings
  import/reconnect regression verifies that a later mailbox-message aggregate
  supersedes the FAI timestamp through the subsequent folder
  `RopGetPropertiesSpecific` and hierarchy ICS, while the hierarchy-table
  regression verifies the same override between adjacent columns. Elimination
  of the Outlook report still requires a real-client rerun. This follows
  `[MS-OXPROPS]` section 2.775, `[MS-OXCFOLD]` section 2.2.2.2.1.14,
  `[MS-OXCFXICS]` section 3.1.5.3, and `[MS-OXCMSG]` section 2.2.1.49.
- The `202607261311` real-Outlook rerun held the synchronization-report count
  at zero through initial connection and ordinary synchronization, then created
  one report only after the reconnect switched the status bar from Microsoft
  Exchange to LPE. During that reconnect Outlook opened the persisted Inbox
  `IPM.Configuration.MessageListSettings` FAI and issued
  `RopFastTransferSourceCopyTo` (`0x4D`) with an empty property-exclusion list.
  LPE's parseable direct `messageContent` omitted `PidTagEntryId`
  (`0x0FFF0102`), although `RopGetPropertiesSpecific` exposed the same object's
  stable 70-byte EntryID and PostgreSQL contained one
  coherent canonical FAI row. This is the first demonstrated projection
  inconsistency in that sequence. LPE now supplies the account-scoped EntryID
  for the special-message families whose existing GetProps projections use
  that format: associated configuration, navigation shortcuts, Common
  Views named views, and the standard `LocalFreebusy` Delegate Information
  object. The shared serializer applies the normal `CopyTo` exclusion /
  `CopyProperties` inclusion filter. It deliberately does not synthesize that
  format for conversation actions or public-folder items, whose existing
  property projections use different identity rules. A realistic
  import/reconnect regression first failed with zero EntryID occurrences and
  now compares the actual CopyTo and GetProps values; ICS regressions
  separately require provider-internal root identity properties to remain
  absent. Filter regressions cover typed and `PtypUnspecified` tags. This
  remains a bounded direct-CopyTo interoperability hypothesis until a real
  Outlook rerun and a separate Exchange control establish that surface. The
  content-sync correction does not treat it as an ICS property. The
  implementation follows `[MS-OXCROPS]` section
  2.2.12.7.1, `[MS-OXCFXICS]` sections 2.2.3.1.1.1.1, 2.2.4.3.16,
  3.2.5.8.1.1, 3.2.5.10, and 3.2.5.12, `[MS-OXPROPS]` sections
  1.3.3 and 2.684, and `[MS-OXODLGT]` section 2.2.2.1.1.
- The `202607261433` rerun then increased the report count from 1 to 2 on the
  first Outlook process, held it at 2 during the same process, and increased it
  to 3 after restarting Outlook. Both process starts produced the same
  `80004002-501-0-0` view/form failure immediately after the Inbox
  `IPM.Configuration.MessageListSettings` direct CopyTo. The corrected
  591-byte payload contained the stable `PidTagEntryId`, proving that the
  preceding bounded correction was necessary but insufficient. No ICS state
  regressed before either failure. The next protocol-defined omission was
  `PidTagParentEntryId` (`0x0E090102`): the containing Inbox Folder EntryID was
  available through GetProps and the associated-contents table. The existing
  direct CopyTo compatibility path retains identification properties, while
  ICS message content omits this provider-internal value. For the observed
  associated-configuration family only, the shared special-message serializer
  now receives the actual account-scoped parent Folder EntryID and applies the same typed or
  `PtypUnspecified` CopyTo exclusion / CopyProperties inclusion rules. A
  realistic Outlook import/reconnect regression first failed with zero
  occurrences and now requires exactly one value equal to both the Inbox
  EntryID and the independent GetProps projection. This is the next bounded
  direct-CopyTo interoperability hypothesis pending a real Outlook rerun and
  separate Exchange control; it is not used as the content-sync rule. It
  follows `[MS-OXPROPS]` sections 1.3.3 and 2.860,
  `[MS-OXCFOLD]` section 2.2.2.2.1.7, and `[MS-OXCFXICS]` sections
  2.2.3.1.1.1.1, 2.2.4.3.16, 3.2.5.8.1.1, 3.2.5.10, and 3.2.5.12.
- The `202607261508` rerun retained the failure after both EntryID corrections.
  PostgreSQL records the first new Deleted Items synchronization report at
  `15:06:21.181`, after the Inbox `MessageListSettings` direct CopyTo completed
  at `15:06:13.133`; the report body's `15:06:12` value is therefore the start
  of the synchronization operation, not its database insertion time. Every ROP
  completed successfully and the collector state was coherent. The first
  remaining cross-surface semantic divergence in the CopyTo payload was
  `PidTagSearchKey`: Outlook's initial import supplied a 16-byte Message search
  identity, while LPE discarded that server-owned input and generated the
  22-byte `PidTagSourceKey` XID as its replacement. The official Microsoft
  MAPI `MAPIUID` definition identifies a Message `PR_SEARCH_KEY` as a 16-byte
  MAPIUID; `[MS-OXCPRPT]` section 2.2.1.9 defines its stable, unique and copied
  semantics, `[MS-OXPROPS]` section 2.999 defines tag `0x300B0102`, and
  `[MS-OXCFXICS]` section 4.5 corroborates the 16-byte Message value in its
  FastTransfer examples. The generated fallback now uses the durable canonical
  UUID as that MAPIUID across direct CopyTo, FAI ICS and table/GetProps
  projections when no imported SearchKey exists. Imported SearchKeys remain
  preserved, and the 22-byte SourceKey, ChangeKey and predecessor list are
  unchanged.
- The clean `202607261926` rerun retained one synchronization report and exposed
  the remaining product-semantic divergence. Outlook imported
  `PidTagSearchKey=711dbcb1d4de79428df00551e825676d`, but LPE discarded it and
  returned `ec2adc4b4cc565fcdcad11588e3a88c6` after reconnect. The direct
  `RopFastTransferSourceCopyTo` request and its 639-byte `messageContent`
  response otherwise decode completely and conform to `[MS-OXCFXICS]` sections
  2.2.4.3.16, 3.2.5.8.1.1, 3.2.5.10, and 3.2.5.12. `[MS-OXCFXICS]`
  section 3.2.5.11 does not place property `0x300B` in the
  provider-defined nontransmittable upload range. `[MS-OXCPRPT]` section
  2.2.1.9 requires a copied SearchKey to remain stable, and `[MS-OXCMSG]`
  section 2.2 product note `<1>` records that Exchange 2010 through 2019 accept
  a change to this otherwise read-only property. A realistic import, Save,
  reconnect, and CopyTo regression now requires the original 16 bytes to remain
  in canonical FAI state. LPE accepts that value only before the first Save;
  later property, stream, and deletion attempts cannot replace it. The
  `202607262126` rerun confirms this correction is necessary but insufficient.
- The clean `202607262126` rerun after that SearchKey correction again held the
  report count at `N0=N1=0` and created one report on the first LPE reconnect
  (`N2=1`). The persisted `MessageListSettings` direct
  `RopFastTransferSourceCopyTo` response decodes completely and preserves the
  imported SearchKey. The following `RopGetPropertiesSpecific` requests
  ChangeKey, PCL, LastModificationTime, private tag `0x0E0B0102`, and
  MessageClass; the absent private tag remains an `ecNotFound` cell.
- The clean `202607271610` rerun again held `N0=N1=0`; Outlook displayed
  `Connected to: Microsoft Exchange` during its first local connection, then
  created `N2=1` only after the second connection displayed
  `Connected to: LPE`. The first effective LPE synchronization exposed a
  reproducible persisted-value divergence in the imported
  `IPM.Configuration.MessageListSettings` FAI. Outlook submitted
  `PidTagCreationTime=2026-07-27T14:08:26.776Z` after
  `RopSynchronizationImportMessageChange` supplied
  `PidTagLastModificationTime=2026-07-27T14:08:27.388Z`; LPE discarded the
  former and returned the latter as both values after reconnect. A PostgreSQL
  regression reproduces that exact Save and reload failure. Although
  `[MS-OXCPRPT]` sections 2.2.1.4 and 3.2.5.4 describe CreationTime as
  read-only, `[MS-OXCMSG]` section 2.2 product note `<1>` records that Exchange
  2010 through 2019 change this property. LPE therefore preserves the initial
  Outlook value at PostgreSQL microsecond precision as its single canonical
  `created_at`; this is an interoperability inference from the product note
  plus the real trace, not an algorithm mandated by the specification. If the
  initial import omits the property, LPE assigns the server creation time.
  Later changes retain the canonical value. The imported LastModificationTime
  remains the independent ICS conflict timestamp required by `[MS-OXCFXICS]`
  sections
  2.2.3.2.4.2.1, 3.1.5.6.2.2, and 3.2.5.9.4.2.
- The `202607272146` rerun held `N0=N1=0` while Outlook displayed
  `Connected to: Microsoft Exchange`. After restart and a real LPE connection,
  two deterministic startup cycles produced `N2=1` and `N3=2`. Both cycles
  read the same persisted Inbox `MessageListSettings` FAI through identical
  639-byte direct `CopyTo` streams before Outlook generated
  `80004002-501-0-0`. CreationTime, LastModificationTime, SearchKey, ChangeKey,
  and PCL were stable. The first remaining imported-value divergence was
  `PidTagLastModifierName` (`0x3FFA001F`): Outlook had supplied
  `test@l-p-e.ch`, while LPE returned the display name `test`.
  `[MS-OXCPRPT]` sections 2.2.1.5 and 3.2.5.4 define the general read-only
  behavior, while `[MS-OXCMSG]` section 2.2 product note `<1>` records the
  Exchange 2010 through 2019 product exception for LastModifierName. LPE now
  projects the owning account's canonical primary SMTP address through
  `GetProps`, tables, direct `CopyTo`, and ICS, while continuing to discard
  arbitrary client-supplied modifier identities. A PostgreSQL-backed regression
  covers Save, process-style reload, and direct FastTransfer replay. Real
  Outlook validation remains required.
- The attempted `ErrorsReturned` interpretation was rejected by the
  `202607262244` real-client run. At `20:43:57.528Z`, the first broad Inbox
  `RopGetPropertiesSpecific` returned a 1,436-byte response with
  `ReturnValue=0x00040380` and complete `RowData`; Windows recorded an
  `EMSMDB32.DLL` access violation 70 ms later. The equivalent
  `202607262126` response is byte-for-byte identical apart from the
  `ReturnValue` and expected handle-table value and did not crash Outlook.
  WER also classified the incident as `OFFICE_MODULE_VERSION_MISMATCH`
  (`OUTLOOK.EXE` 16.0.20131.20154 versus `EMSMDB32.DLL`
  16.0.20131.20044), so the warning is the first changed wire input and a
  trigger candidate, not a proven native root cause without a dump.
  `[MS-OXCROPS]` sections 2.2.8.3.2 and 2.2.8.3.3 define success with
  `ReturnValue=0` plus `RowData`, or a nonzero failure without `RowData`.
  `[MS-OXCDATA]` section 2.4.3 gives a conflicting generic warning example.
  Until an Exchange reference capture resolves that conflict, LPE follows the
  ROP-specific wire contract and retains per-property errors in the
  `FlaggedPropertyRow` without changing the overall success value.
- The `202607270652` rerun no longer crashed Outlook after restoring that
  ROP-specific success contract, but each of its two LPE reconnects still
  produced one `80004002-501-0-0` view/form report. Correlation with the clean
  `202607262126` profile creation identifies the earlier semantic divergence.
  Outlook imported five Inbox FAI messages with client
  `PidTagLastModificationTime` values ending at `19:24:12.630Z`, while their
  server Saves continued through approximately `19:24:14.753Z`. Three
  subsequent `PidTagLocalCommitTimeMax` reads still returned
  `19:24:12.630Z`, the maximum client-imported modification time rather than
  the later canonical server commit.
  The later ExtendedRule only masked that stale Inbox maximum after the first
  error had already occurred. An imported FAI LastModificationTime remains the
  replicated version time used for conflict semantics; it is not the local
  store commit performed by `RopSaveChangesMessage`. For a committed FAI
  content change, LPE now obtains the contribution to
  `PidTagLocalCommitTimeMax` from the canonical
  `mapi_associated_config_messages.updated_at` value exposed in the snapshot as
  `__lpe_updated_at`. The `202607270652` run reproduces the report symptom, not
  this now-masked Inbox watermark itself, so elimination still requires a new
  database and profile. The realistic import/Save/reconnect regression keeps
  `PidTagLastModificationTime` at the imported value while requiring both the
  Message `PidTagLocalCommitTime` and the containing folder
  `PidTagLocalCommitTimeMax` to expose the later server commit; a PostgreSQL
  regression verifies the same aggregate after a real store reload. This follows
  `[MS-OXCMSG]` sections 2.2.1.49 and 3.2.5.3, `[MS-OXCFOLD]` sections
  2.2.2.2.1.13 and 2.2.2.2.1.14, and `[MS-OXCFXICS]` section 3.1.5.3.
- The `202607272220` rerun increased the existing synchronization-report count
  from `N0=2` to `N1=3` and `N2=4`. Both failures followed the same read-only
  (`OpenModeFlags=0x00`) Inbox `IPM.Configuration.MessageListSettings` open,
  direct `CopyTo`, `GetProps`, and collector transfer-state sequence. All 223
  captured HTTP exchanges, MAPI response codes, and decoded ROP return values
  succeeded; `0x80004002` was generated locally by Outlook and means
  `NoInterface` under `[MS-OXCDATA]` section 2.4. The 657-byte FastTransfer
  streams decode completely and are identical. `PidTagAccessLevel=0` is
  therefore consistent with the read-only message handle and is not a defect.
  The persisted FAI's collector state also contains the five server change
  numbers assigned to the preceding Outlook imports in
  `MetaTagCnsetSeenFAI`, as required by `[MS-OXCFXICS]` section 3.1.5.3.
  Comparison with `202607272146` instead proves that the deployed projection
  changed `PidTagLastModifierName` and `PidTagMessageSize` while retaining the
  same `PidTagChangeKey`, `PidTagPredecessorChangeList`, change number, and
  `PidTagLastModificationTime`. That existing database and OST can no longer
  validate the correction: `[MS-OXCFXICS]` sections 2.2.1.2.7, 2.2.1.2.8, and
  3.1.5.3 require the version identifiers to identify the current object
  version and a new change number for each modification. The next validation
  must use an empty `0.5.2-sql` database and a new Outlook profile/OST, without
  another projection change first.
- The clean `202607280707` validation used an empty canonical `0.5.1-sql`
  database and a new Outlook profile/OST. It held the synchronization-report
  count at `N0=N1=0` and produced one report after the first reconnect
  (`N2=1`), falsifying the stale-version hypothesis. The imported Inbox
  `IPM.Configuration.MessageListSettings` FAI retained one stable MID, SourceKey,
  SearchKey, ChangeKey, PCL, LastModificationTime, CreationTime,
  LastModifierName, MessageFlags, and content bag. Outlook's read-only
  `OpenMessage`, direct `RopFastTransferSourceCopyTo`, follow-up GetProps, and
  collector transfer-state ROPs all returned success. The report upload began
  4.16 seconds after the 657-byte direct CopyTo completed. The same object had
  already been accepted through ICS with its containing Inbox
  `PidTagParentSourceKey` (`0x65E10102`), while the direct CopyTo omitted that
  value despite an empty exclusion list. LPE also exposes that containing-folder
  identity through Message GetProps and tables. The special-message direct
  serializer now projects exactly one identical ParentSourceKey for Outlook
  configuration FAI messages and applies the normal typed or
  `PtypUnspecified` CopyTo exclusion / CopyProperties inclusion filters. Other
  FAI families retain their existing direct projection. The expected direct
  stream is therefore 687 bytes; CN, CK, PCL, timestamps, content, and transfer
  state are unchanged. `[MS-OXPROPS]` section 2.863 defines ParentSourceKey
  canonically on folders, so this Message projection is an explicit LPE/Outlook
  interoperability contract rather than an unconditional protocol requirement.
  The CopyTo property-selection behavior follows `[MS-OXCFXICS]` sections
  2.2.3.1.1.1.1, 2.2.4.3.16, 3.2.5.8.1.1, 3.2.5.10, and 3.2.5.12. Elimination
  of the report requires a real Outlook rerun.
- The clean `202607280946` rerun retained `N0=N1=0` and produced `N2=1`,
  falsifying the direct `PidTagParentSourceKey` omission as the cause of the
  report. Both direct `MessageListSettings` CopyTo payloads are 687 bytes and
  byte-for-byte identical apart from their transient response handle-table
  values. Each contains the exact Inbox ParentSourceKey, and every ROP and MAPI
  HTTP response succeeds. The first CopyTo completed at
  `2026-07-28T07:45:39.647Z`; Outlook's compressed-RTF synchronization report
  records `80004002-501-0-0` (`NoInterface`) during that same local
  view/form merge. Outlook nevertheless imported and saved one new
  `IPM.Configuration.AccountPrefs` FAI and reported that one view/form was
  added to the online folder before it uploaded the report to Deleted Items.
  The two follow-up property probes are also identical: ChangeKey, PCL,
  LastModificationTime, and MessageClass succeed, while private binary tag
  `0x0E0B0102` returns an `ecNotFound` property cell. That property ID is not
  defined by `[MS-OXPROPS]`, so this trace alone did not justify synthesizing a
  value.
  The zero MessageId in the successful
  `RopSynchronizationImportMessageChange` response is also not a defect:
  `[MS-OXCROPS]` section 2.2.13.2.2 requires that field to be zero. The
  remaining failure is local to Outlook's import/merge of an otherwise
  parseable direct `messageContent`.
- Exchange reference captures at `202607281118` and `202607281134`, using
  Outlook `16.0.20131.20044` against Exchange `15.01.2507.034`, contain the
  exact post-save `MessageListSettings` GetProps probe. Exchange returns
  `0x0E0B0102` successfully as the same 46-byte binary value in both captures
  while ChangeKey, PCL, and LastModificationTime advance; it returns
  `0x664F000B` as `ecNotFound`. The private value has the account-scoped parent
  Folder EntryID shape with entry type `0x000D`, the store replica GUID, and the
  parent folder global counter. This proves that LPE's `ecNotFound` result is a
  real Exchange divergence even though the tag remains absent from public
  `[MS-OXPROPS]`. LPE now computes that bounded property only for
  `IPM.Configuration.MessageListSettings` GetProps, table, and stream access.
  Those two Exchange captures do not contain `RopFastTransferSourceCopyTo`.
  Contrary to an earlier reading, neither does the later Exchange 2016
  reference capture `test1_202607281754.saz`; it exercises the normal
  synchronization and client-save path but contains no `0x4D` ROP. Those
  captures therefore establish only the post-save GetProps result, not a
  corresponding direct FastTransfer property.
- The `202607312152` LPE trace shows that recovery path directly: Outlook opens
  the persisted Inbox `IPM.Configuration.MessageListSettings` FAI, sends
  `RopFastTransferSourceCopyTo` with an empty exclusion list, and immediately
  asks `RopGetPropertiesSpecific` for `0x0E0B0102`. LPE returned the
  account-scoped 46-byte value to GetProps but omitted it from the preceding
  direct `messageContent`; all ROP return values were successful, so Outlook
  wrote its own `Synchronization Log:` report to Deleted Items while merging
  the inconsistent payload. LPE temporarily used the same scoped projection
  for this property in direct associated-configuration CopyTo as in GetProps.
  That was a bounded cross-surface consistency inference from the Exchange
  GetProps result, not an observed or protocol-defined FastTransfer
  requirement. The raw `CnsetSeenFAI` state was also checked:
  the earlier diagnostic range summary was truncated, but the wire blob already
  acknowledged every imported FAI change number, so no state-handling change is
  justified from this trace.
- The `202607281300` rerun confirms the `0x0E0B0102` correction on wire:
  Outlook's direct post-CopyTo probe receives a successful 46-byte value with
  no problem cells, yet `N2=1` remains and both direct CopyTo payloads are
  unchanged from `202607280946`. The Exchange probe also returns
  `PidTagMessageStatus` (`0x0E170003`) as zero, while LPE's same recovery
  payload preserved an imported value of one. LPE now normalizes that property
  to zero only for `IPM.Configuration.MessageListSettings`. The same
  projection applies to direct `CopyTo`, preventing a persisted client value
  from contradicting the subsequent `GetProps` response.
- Probe I (`202608130758`) falsifies the temporary direct `0x0E0B0102`
  projection. Outlook's requests `:9` through `:13` open the persisted Inbox
  `IPM.Configuration.MessageListSettings`, request direct CopyTo with
  `CopyFlags=0x00002000`, `SendOptions=0x09`, and an empty exclusion list, read
  one complete 919-byte `messageContent`, then successfully read the private
  property through `RopGetPropertiesSpecific`; every ROP succeeds, but Outlook
  still records local `80004002-501-0-0`. The direct stream contains the
  computed 46-byte `0x0E0B0102` even though Outlook did not persist it. The
  earlier LPE direct associated-configuration captures in
  `logs/202608041041.saz` raw exchanges `081` and `178` use the identical CopyTo
  request and emit the same general FAI and named content metadata without
  `0x0E0B0102`. Those LPE captures were previously misclassified as Exchange
  controls. They open `IPM.Configuration.Autocomplete`, so they establish
  neither the Exchange MessageListSettings property set nor an Exchange direct
  CopyTo precedent. LPE therefore retains the independently observed
  MessageListSettings GetProps/table projection while no longer synthesizing
  it on direct FastTransfer surfaces. A client-persisted value remains ordinary
  canonical FAI content. This follows the direct `messageContent` root and
  property-selection rules in `[MS-OXCFXICS]` sections 2.2.3.1.1.1.1,
  2.2.4.3.16, 3.2.5.8.1.1, 3.2.5.10, and 3.2.5.12. A fresh Outlook run is
  required to determine whether the speculative property caused the local
  view/form merge failure.
- The Exchange 15.1.2507.34 root-store probes in `202607281134` return
  `MAPI_E_NOT_FOUND` for `PidTagServerTypeDisplayName` (`0x341D001F`),
  `PidTagServerConnectedIcon` (`0x341E0102`),
  `PidTagServerAccountIcon` (`0x341F0102`), and `PidTagOutlookStoreState`
  (`0x346F0003`); LPE now returns the same absent-property result instead of
  synthesizing values. The traced `PidTagMaxSubmitMessageSize` is 25 KiB.
- The same Exchange capture returns the 46-byte folder-derived `0x0E0B0102`
  EntryID for `IPM.ExtendedRule.Message` as well as
  `IPM.Configuration.MessageListSettings`; LPE projects it for both traced
  associated-message classes.
- The clean `202607291055` run again held `N0=N1=0` and produced
  `N2=1`. Before the later Inbox `MessageListSettings` CopyTo, Outlook wrote
  only indexes 0 through 3 of Inbox `PidTagAdditionalRenEntryIds`. LPE had
  initially projected five entries, but persisted the four-entry prefix and
  later returned it unchanged. The comparable Exchange Inbox probe retained the
  five documented folder EntryIDs plus its existing opaque sixth value. Under
  `[MS-OXOSFLD]` section 2.2.4, LPE now projects the documented five entries
  canonically and merges only later opaque profile values under the durable
  Inbox version lock. Existing missing, four-entry, and stale-alias profile rows
  require no schema migration: before export they are lazily materialized or
  normalized, rotated, and journaled once with hierarchy projection version 4,
  so an old OST's `CnsetSeen` cannot suppress the repaired folderChange. The
  regression exercises Outlook's exact four-entry
  write, persistence, and reconnect. This corrects a
  documented state-corruption divergence; a fresh Outlook run must still prove
  whether it eliminates the local view/form report.
- The clean `202607291330` run still held `N0=N1=0` and produced `N2=1`.
  Comparing its pre-error sequence against the Exchange 2016 reference
  `test1_202607281134.saz` found two remaining direct wire differences. The
  Exchange `RopOpenMessage` response for ASCII
  `IPM.Configuration.MessageListSettings` strings uses reduced Unicode
  `TypedString` (`0x03`), while LPE used full Unicode (`0x04`). Both forms are
  permitted by `[MS-OXCDATA]` section 2.11.7; LPE now selects the reduced form
  for that observed Inbox message class only when it losslessly represents
  every UTF-16 code unit. In the exact
  Inbox `RopGetPropertiesSpecific` form probe, Exchange returns
  `MAPI_E_NOT_FOUND` for the unpersisted `PidTagDefaultPostMessageClass`
  (`0x36E5001F`) whereas LPE synthesized `IPM.Note`. The initial response-path
  guard was shadowed by the synthetic value in the opened Inbox projection.
  LPE now removes that projected value, so the existing absent-property path
  returns the same error while a value set on the active folder handle remains
  available. The other apparent form/view candidates already
  match Exchange's absent-property cells under `[MS-OXCROPS]` section
  2.2.8.3.2. This aligns the demonstrated responses; a new Outlook run is
  still required to establish whether it removes `N2`.
- `RopSynchronizationConfigure` and `RopFastTransferSourceGetBuffer` require
  strict request and response framing. Any parser extension must be validated
  with deterministic golden vectors or local protocol builders.
- The clean `202607291643` run still produced `N0=N1=0`, `N2=1`. The exact
  32-tag Inbox `RopGetPropertiesSpecific` request in the Exchange 2016
  `test1_202607281134.saz` reference (`raw/249`) returns
  `PidTagRights (0x66390003) = 0x000007FB`; LPE had returned the unrelated
  `PidTagAccess` mask `0x0000003F`. `PidTagRights` uses the
  `PidTagMemberRights` permission format, not the access-mask format. LPE now
  projects `PidTagAccess` and `PidTagRights` separately, derives shared/public
  folder rights from their canonical grants, and includes the required
  `EditOwned` and `DeleteOwned` bits whenever it grants `EditAny` and
  `DeleteAny`. The exact captured request is a regression test. This is a
  required Exchange convergence correction, but a fresh Outlook profile run is
  still required to determine whether it removes the local view/form report.
  This follows `[MS-OXCFOLD]` section 2.2.2.2.2.8, `[MS-OXCPERM]` section
  2.2.7, and `[MS-OXPROPS]` section 2.937.
- The same Exchange `raw/249` response marks `PidTagRemOfflineEntryId`
  (`0x36D60102`) and `PidTagArchivePeriod` (`0x301E0003`) with
  `MAPI_E_NOT_FOUND`. LPE had synthesized a second Reminders EntryID and a
  zero folder archive period. `[MS-OXOSFLD]` section 2.2.3 lists only the
  online Reminders EntryID, while `[MS-OXCMSG]` section 2.2.1.60.7 permits
  ArchivePeriod on folders without assigning it a generic default or special
  folder significance. LPE now
  leaves both unpersisted folder properties absent, including from default
  folder projections; an explicitly set value on an open folder remains
  readable. The exact 32-tag regression asserts the two additional absent
  cells. This change has no database or migration requirement.
- The same `raw/249` reference has a material `PidTagFreeBusyEntryIds`
  (`0x36E41102`) divergence: Exchange returns `[null, Delegate Information
  Message EntryID, null, Freebusy Data Folder EntryID]`, while LPE had returned
  a null second element. `[MS-OXOSFLD]` section 2.2.6 requires that second
  EntryID to target the Delegate Information object. LPE now uses its existing
  read-only `LocalFreebusy` projection—the required
  `IPM.Microsoft.ScheduleData.FreeBusy` / `LocalFreebusy` object under
  `[MS-OXODLGT]` sections 2.2.2.1.1 and 2.2.2.1.2—as that stable target. The
  same 70-byte account-scoped EntryID is returned by GetProps and tables.
  Direct CopyTo and normal-content sync keep provider-local EntryID and
  InstanceKey out of the message content; sync identifies the same object by
  its durable SourceKey and optional MID instead. The private `0x0E0B0102`
  46-byte value is likewise limited to the observed GetProps/table/stream
  surface because no Exchange LocalFreebusy FastTransfer control supports
  transmitting it. Probe K supersedes the
  earlier constant virtual identity: the object now uses one durable
  account-scoped MAPI identity/version row, while its delegate-property content
  remains a projection of canonical grants, rights, and preferences. With no
  configured delegates, the Exchange `raw/548` property vector returns the
  first fourteen Delegate Information cells as `ecNotFound`; configured rows
  project correlated names, EntryIDs, flags, and delivery preferences. The
  valid empty appointment-tombstone stream remains available. This requires no
  LocalFreebusy content table, but it does require the fresh-schema delegation
  projection revision and durable MAPI identity metadata described above.
- Exchange's additional `PidTagExtendedFolderFlags` (`0x36DA0102`) subproperty
  in that same request is a reserved `0x06` record under `[MS-OXOCFG]` section
  2.2.7.1, with no specified meaning. LPE's default valid `0x01` record is
  retained; it already preserves a client-supplied complete blob across reopen,
  so it must not synthesize Exchange's opaque reserved trailer globally or for
  Inbox alone.
- Per `[MS-OXCROPS]` section 2.2.13.1.1,
  `RopSynchronizationConfigure` always carries `RestrictionDataSize`,
  `SynchronizationExtraFlags`, `PropertyTagCount`, and `PropertyTags`; the
  parser must consume those fields before reading the next ROP in a batch.

`mapi_mailstore.rs`, `mapi_mailstore/manifest.rs`, and `mapi/sync.rs` reached the
thousand-line split threshold. Shared `SpecialMessageSyncFact` property
selection and FastTransfer serialization now live in
`mapi_mailstore/special_message.rs`; associated-configuration projection lives
in `mapi/sync/associated_config.rs`, and this patch moves
`calendar_sync_object` into `mapi/sync/calendar.rs`. Keep further behavior in those focused
helpers and the public entry points as thin wiring. Verify changes with the
special-message unit tests, the realistic `MessageListSettings`
import/reconnect regression, and `cargo test -p lpe-exchange`.
`mapi/dispatch/sync_import.rs` and `mapi/dispatch/table_diagnostics.rs` have
also crossed the thousand-line threshold. Before expanding either again, move
upload-checkpoint state recording into `mapi/dispatch/sync_import/state.rs` and
Common Views diagnostic formatting into
`mapi/dispatch/table_diagnostics/common_views.rs`; leave the current files as
dispatch and aggregation wiring.

### Table Projection Contract

Table projection must produce parseable Outlook-compatible rows from canonical
state. The supported projection surface is:

| Table surface | Required behavior |
| --- | --- |
| Hierarchy tables | Root/IPM subtree child folders, special folder identity, source keys, change keys, predecessor lists, display names, container class, content counts, unread counts, replica fields, and folder child counts. |
| Contents tables | Folder membership rows with stable message identifiers, source/change keys, predecessor lists, subject, dates, sender, recipients where supported, flags, message class, read state, size, and attachment indicators. |
| Attachment tables | Canonical attachment rows with stable attachment numbering and properties required by Outlook cached-mode reads. |
| Permission tables | Canonical permission projection plus bounded mutation through `mailbox_delegation_grants`; no MAPI-local ACL table is allowed. |
| Search and reminder folders | Persisted canonical built-in and user-saved search-folder definitions plus hierarchy/content projections; Common Views search-definition FAI rows are published only when a stored `[MS-OXOSRCH]` BLOB has the advertised required blocks. |

Categorized contents tables are bounded to the canonical rows already available
through the table projection. `RopSortTable` category counts create
session-local category metadata on the table handle, `RopQueryRows` emits
category header rows and expanded leaf rows from canonical contents rows,
`RopExpandRow` and `RopCollapseRow` update only that table handle, and
`RopGetCollapseState` / `RopSetCollapseState` serialize and restore a bounded
collapse-state blob for the active table. LPE does not persist categorized
collapse state as profile data unless a future architecture update explicitly
defines it as bounded profile state.

### Specification Basis

The wire contract is based on Microsoft MAPI over HTTP, EMSMDB, NSPI, ROP,
FastTransfer/ICS, store object, folder, property, special folder, search folder,
reminder, notes, journal, task, and free/busy protocol documentation reviewed
for this implementation plan. The plan treats those documents as protocol
requirements, not as permission to introduce Exchange-only stores or
non-canonical LPE state.

## Canonical LPE Mapping Decisions

- LPE remains the canonical store for mailboxes, contacts, calendars, tasks,
  search, rights, submission, and user-visible state. MAPI over HTTP is an
  authenticated compatibility surface over that state.
- Client-facing SMTP submission stays outside the core LPE server. MAPI must use
  canonical LPE submission and must not implement client SMTP submission.
- MAPI must not maintain protocol-local `Sent`, `Outbox`, draft, attachment,
  folder, search-folder, public-folder, reminder, or address-book truth.
- Any message sent from Outlook must be recorded by canonical LPE submission and
  visible in canonical `Sent`. Cross-protocol checks must agree through JMAP,
  IMAP where applicable, and the MAPI projection.
- For Outlook cached-mode optimizing send, a valid `PidTagTargetEntryId` for
  this mailbox's Outbox creates a transient canonical Outbox membership for the
  same message submitted to canonical `Sent`; it must not create a second
  message or a protocol-local Sent/Outbox store. When Outlook later imports the
  local Sent move, LPE atomically removes that Outbox membership and rekeys the
  active MAPI message identity to the imported destination SourceKey,
  ChangeKey, and PredecessorChangeList while allocating a distinct server
  ChangeNumber. This implements the duplicate-upload avoidance sequence in
  `[MS-OXCFXICS]` sections 3.3.4.3.3.2.1.1 and 3.3.4.3.3.2.1.2 and the
  submission optimization in `[MS-OXOMSG]` sections 3.2.4.4 and 3.3.5.1.3.
  Immediately after that move, a normal-message
  `RopSynchronizationImportMessageChange` can reannounce its already-durable
  `PidTagSourceKey`, `PidTagLastModificationTime`, `PidTagChangeKey`, and
  `PidTagPredecessorChangeList`; LPE ignores those server-managed headers only
  on that ICS path without changing ordinary property-staging behavior.
- MAPI submission cancellation is canonical queue cancellation, not message
  deletion and not a client-spooler side channel. `RopAbortSubmit` resolves the requested folder/message identifiers to the
  authenticated account's canonical `Sent` membership and then to the matching
  `submission_queue` row. It may transition only `queued`, `ready`, or
  `deferred` rows to terminal `cancelled`, set `terminal_at`, append a
  `submission_events.cancelled` row, write the canonical submission change-log
  event, and wake normal mailbox-change listeners. It must not remove the
  authoritative `Sent` copy, recreate the source draft, mutate recipients,
  cancel `handed_off` transport custody, or recall an already relayed message.
  Already terminal rows stay terminal; duplicate cancellation of an already
  `cancelled` row is idempotent, while `handed_off`, `relayed`, `bounced`, and
  `failed` rows return a parseable cannot-abort ROP error without side effects.
- Draft save, send, move, copy, delete, read/unread, flag, attachment, and
  protected-recipient behavior must map to canonical mailbox state.
  `PidTagMessageFlags` (`0x0E070003`) projects `mfUnsent` (`0x00000008`)
  whenever the current canonical mailbox membership is a draft, as required by
  `[MS-OXCMSG]` section 2.2.1.6; a Drafts table row must never claim sent state.
- `Bcc` is protected metadata. It must not leak through MAPI search, AI-facing
  indexing, non-owner projections, or protocol shortcuts.
- NSPI resolves the authenticated mailbox and visible contacts from canonical
  account/contact visibility. `ModLinkAtt`, `ModProps`, and other NSPI mutation
  surfaces remain disabled until canonical write semantics are explicitly
  designed.
- NSPI `DNToMId` is authenticated but stateless so Outlook's late bootstrap
  name-resolution probes can complete after session rotation. NSPI `Unbind` is
  idempotent for already-removed session cookies and clears the session cookies;
  stateful NSPI table/property operations still require a live bound session.
- Outlook default-folder properties must be projected from canonical folder
  identities on both the Inbox and Root folder. Generated special-folder binary
  identifiers use the documented 46-byte folder EntryID form for the scalar
  special-folder properties, including IPM subtree, Outbox, Deleted Items, Sent
  Items, Views, Common Views, Finder/Search, Archive, Calendar, Contacts,
  Journal, Notes, Tasks, Reminders, and Drafts. Cached 24-byte LongTermIDs and
  46-byte folder EntryIDs written back by Outlook remain accepted, normalized,
  and retained on the live root-folder handle for cached-mode bootstrap, while
  canonical projection still wins after reconnect.
- `PidTagAdditionalRenEntryIds` is canonically an Inbox special-folder
  identification property under `[MS-OXOSFLD]` section 2.2.4. Outlook 2016/2019
  cached-mode startup can write a partial indexed value to the Inbox or the same
  indexed values to the Root handle after hierarchy sync. LPE always projects
  indexes 0 through 4 as the canonical special-folder EntryIDs, and merges only
  opaque later positions into the existing profile value. Omitted documented
  positions and opaque later positions remain intact. The resulting Inbox value
  is bounded durable compatibility metadata, not canonical folder or
  user-visible state. A Root-handle write is normalized into that same Inbox
  compatibility value; Root has no independent persisted ownership or
  advertised value. The normalized value is also emitted in the Inbox
  hierarchy `folderChange`. A direct Root/Inbox write and an existing-Inbox
  hierarchy import atomically commit that value, any validated aliases, the
  Inbox CN/ChangeKey/PCL/LMT tuple, and one MAPI-only hierarchy replay row.
- `RopIdFromLongTermId` advertises the canonical store replica GUID in
  `PidTagSerializedReplidGuidMap`, but it also accepts the authenticated
  mailbox account GUID byte layouts as legacy replica aliases so stale Outlook
  special-folder caches can resolve back to canonical LPE folder IDs. If a
  cached LongTermID carries another stale store GUID, LPE accepts it only when
  the global counter maps to an LPE-advertised MAPI special folder; normal
  mailbox items still require the canonical store replica GUID or authenticated
  mailbox GUID.
- Search folders are canonical persisted definitions and computed folder
  projections. Built-in definitions cover Outlook bootstrap surfaces such as
  To-Do, Tracked Mail Processing, Contacts Search, and Reminders, but LPE does
  not export them as Common Views FAI definition messages unless the definition
  carries a stored `[MS-OXOSRCH]` BLOB whose advertised `FolderList2` and
  `SearchRestriction` blocks are present. LPE must not synthesize partial
  `IPM.Microsoft.WunderBar.SFInfo` rows from incomplete canonical search JSON.
  Search-folder hierarchy and contents remain canonical projections; Common
  Views search-definition FAI rows must not be invented from LPE-private JSON.
  User-saved definitions project as MAPI `FOLDER_SEARCH` hierarchy rows with
  stable canonical identities and container classes derived from their canonical
  result object kind.
- MAPI projects Outlook's default Contacts and Calendar folders even when the
  account has no canonical contact or calendar collections yet. Those empty
  folder projections use reserved MAPI counters, stable source keys, and virtual
  folder checkpoint scopes; they must not create canonical collections until a
  user/API action creates real collaboration state. Outlook may write back or
  cache the Calendar default-folder EntryID during profile bootstrap, so the
  advertised folder must have durable MAPI backing even when the calendar has no
  events yet. A MAPI `IPM.Appointment` create/save against that advertised empty
  Calendar folder creates the event through canonical calendar storage using
  the default calendar collection, not through MAPI-local item state. Existing
  events in that implicit default collection can be read, updated, deleted, and
  opened with canonical attachments through the advertised Calendar folder. If
  collection discovery returns no explicit default Calendar row but canonical
  events already reference the default calendar collection, MAPI sync and
  selective object loads still project those events through the advertised
  Calendar folder. LPE does not
  synthesize Calendar configuration FAI rows during
  first sync. `[MS-OXOCFG]` defines how `IPM.Configuration.Calendar`,
  `IPM.Configuration.CategoryList`, and `IPM.Configuration.WorkHours` messages
  are stored when configuration data exists, but partially fabricated bootstrap
  configuration rows are not canonical calendar state and are unsafe for
  Outlook's initial Calendar FAI parser. Fresh-profile Calendar FAI content sync
  is therefore allowed to be state-only until Outlook creates real associated
  configuration messages that LPE can persist and replay.
  The `202607291610` first-profile trace shows that Outlook's local
  "Upgrading Calendar Labels for Color Categories" UI creates the Inbox FAI
  markers `IPM.Microsoft.PendingChange.MigrateCategoriesList` and
  `IPM.Microsoft.MigrationStatus`; it does not send a
  `IPM.Configuration.CategoryList` operation. Those client migration markers
  must be persisted and replayed, not replaced with a server-synthesized
  category list.
- Conversation Action Settings exposes only FAI rows projected from canonical
  `conversation_actions` records. With no canonical action, its
  associated-contents table is empty and its ICS stream is state-only; LPE must
  not invent a default
  `IPM.ConversationAction` with a nil conversation identity. `[MS-OXOCFG]`
  sections 2.2.8, 2.2.8.8, and 2.2.8.10 describe the FAI's shared conversation
  ID and correlating subject, while section 3.1.5.1 specifies that no action is
  processed when no matching FAI exists.
- Content synchronization emits long-term `PidTagEntryId` values for message
  and FAI rows using the documented private mailbox Message EntryID shape:
  mailbox account GUID as provider UID, canonical store replica GUIDs, and the
  folder/message global counters used by `PidTagSourceKey`. Outlook relies on
  this identity material when deriving local item-friendly identifiers during
  cached-mode sync.
- Private-mailbox `RopLogon` responses expose the Microsoft fixed folder-id
  slots before `ResponseFlags` and `MailboxGuid`, including the `Shortcuts`
  slot required by current Outlook clients. The corresponding Root hierarchy
  table also exposes these documented Root children, including Schedule, Search,
  Common Views, Personal Views, and Shortcuts. The `Shortcuts` FID is a bounded
  openable Root child for Outlook startup compatibility; it does not create a
  canonical LPE shortcut store.
- Common Views, Schedule, Search, Personal Views, and Shortcuts are Root
  children outside the IPM subtree.
  Navigation shortcuts are projected as folder-associated information messages
  in Common Views, not as durable contents in the Shortcuts folder. This follows
  `[MS-OXOSFLD]` sections 2.2.2 and 3.1.1.1 for special-folder behavior and
  `[MS-OXOCFG]` navigation shortcut semantics: a shortcut is a Common Views FAI
  message with `WLink` properties.
- LPE does not publish synthetic `IPM.Microsoft.FolderDesign.NamedView` rows or
  default-view EntryIDs that resolve only to virtual messages. That former
  projection was removed because it could leave Outlook with dangling EntryIDs;
  a NamedView is exposed only when a corresponding canonical FAI message exists.
  Outlook-created or imported associated configuration rows remain durable, but
  Inbox startup enumeration does not replay broad persisted `IPM.Configuration.*`
  rows or `IPM.ExtendedRule.Message` rows. In particular, LPE does not synthesize
  an empty `IPM.Configuration.MessageListSettings` row for the broad startup
  prefix probe: Exchange 2016 capture `202607281754` follows the normal
  synchronization and client-save path, whereas LPE's synthetic row led Outlook
  to its failing CopyTo recovery path. Only Outlook-created or imported durable
  configuration rows are exposed; exact, bounded lookups retain their supported
  virtual projections.
- `RopSaveChangesMessage` commits accepted `RopSetProperties` mutations
  according to `[MS-OXCPRPT]` section 3.2.5.4. When Outlook
  imports the Inbox `MessageListSettings` FAI, LPE preserves its imported
  SourceKey, LastModificationTime, ChangeKey, PCL, MID, initial CreationTime,
  and exact client-written content. It projects LastModifierName from the owning
  account's canonical primary SMTP address and never persists a client-selected
  modifier identity. The initial CreationTime and LastModifierName behavior
  follows the Exchange product behavior documented by `[MS-OXCMSG]` section 2.2
  note `<1>` together with the general read-only rules in `[MS-OXCPRPT]`
  sections 2.2.1.4, 2.2.1.5, and 3.2.5.4. When CreationTime is absent, LPE
  assigns the server creation time. Later imports retain it, independently of
  the imported LastModificationTime.
  For a committed FAI content change, the distinct Message LocalCommitTime and
  containing-folder LocalCommitTimeMax contribution use the canonical server
  commit time, not that imported LastModificationTime. An
  explicit zero `PidTagRoamingDatatypes` remains zero and absent roaming
  streams remain absent. The undocumented `0x0E0B0102` value is neither stored
  nor transferred; it remains a bounded MessageListSettings
  `RopGetPropertiesSpecific` projection matching the Exchange controls.
  `PidTagRoamingDatatypes` governs only those
  roaming streams; named properties remain part of the persisted client
  property bag. Probe L (`202608131831`) imported MessageListSettings without
  `PidNameContentClass` or `PidNameContentType`, while Probe M
  (`202608131919`) exposed both values being added by LPE's former class-based
  fallback. The genuine Exchange `test1_202608031300.saz` raw/194, raw/466,
  raw/512, and raw/688 controls set `HasNamedProperties=1`, but their requested
  property vectors do not identify a named property and they contain neither
  GetPropertiesAll nor direct CopyTo. LPE therefore advertises and exports only
  named properties actually stored on the FAI; the Exchange flag alone does
  not authorize substituting guessed identities. This follows [MS-OXCMSG]
  section 2.2.3.1.2 and the property-filter boundary in [MS-OXCFXICS] sections
  2.2.4.3.16 and 3.2.5.10. Reconnect and direct CopyTo retain the persisted
  configuration-property set. The server owns and sets
  `PidTagMessageFlags.mfFAI` and `mfEverRead`, but retains the other flag bits
  accepted before the first successful Save: persisted/client `0x00000049` is
  therefore exported as `0x00000449`, not reduced to `mfFAI` alone. Both ICS
  content download and direct Message CopyTo omit `PidTagObjectType` and
  `PidTagRecordKey` from FAI Message objects. Direct Message CopyTo retains
  `PidTagSourceKey`, `PidTagEntryId`, `PidTagSearchKey`, `PidTagChangeKey`, and
  `PidTagPredecessorChangeList`, but does not synthesize the folder-only
  `PidTagParentSourceKey`; an explicitly persisted Message value remains part of
  the filtered property bag. ICS retains its separate ParentSourceKey
  projection. This follows `[MS-OXCMSG]` section 2.2.1.1
  product notes 2 and 3; `[MS-OXPROPS]` section 2.912 identifies
  `PidTagRecordKey`, while `[MS-OXCFXICS]` sections 2.2.4.3.13 and 2.2.4.3.16
  define the ICS and direct FastTransfer message surfaces. An absent
  `PidTagBody` remains absent from `BestBody` CopyTo output; an explicitly empty
  body remains a present zero-length value. A direct
  `RopGetPropertiesSpecific` returns a requested property determined to be
  absent as an `ecNotFound` cell in a `FlaggedPropertyRow`, with
  `ReturnValue=Success` following the response-specific `[MS-OXCROPS]` section
  2.2.8.3.2. `[MS-OXCDATA]` section 2.4.3 instead gives a generic
  `ErrorsReturned` example; the `202607262244` run places that nonzero value
  with `RowData` immediately before the correlated Outlook crash. The
  property-row encoding follows `[MS-OXCPRPT]` sections 2.2.1.4, 2.2.1.6, 2.2.2,
  2.2.2.2, 3.2.5.1, and 3.2.5.4, `[MS-OXCDATA]` sections 2.4.2, 2.8.1,
  2.8.1.2, and 2.11.5,
  `[MS-OXCMSG]` sections 2.2.1.6 and 3.2.5.3, `[MS-OXOCFG]` sections 2.2.2.1 through
  2.2.2.3, 2.2.5.1, and 2.2.5.2, `[MS-OXPROPS]` sections 2.938 through 2.940,
  and `[MS-OXBBODY]` section 2.1.3.1. The FastTransfer behavior follows
  `[MS-OXCFXICS]` sections
  2.2.3.1.1.1.1, 2.2.3.2.4.2.1, 2.2.4.3.16, 2.2.4.4, 3.1.5.3,
  3.1.5.6.2.2, 3.2.5.8.1.1, 3.2.5.9.4.2, 3.2.5.10, and 3.3.5.8.7.
  Mutations to one open associated-configuration message are cumulative on that
  message handle through `RopSaveChangesMessage`; a later `RopSetProperties` or
  `RopDeletePropertiesNoReplicate` in the same batch must use the updated handle
  state rather than an older mailbox snapshot. This follows `[MS-OXOCFG]`
  section 3.1.4.2 and the property/message ROP contracts in `[MS-OXCROPS]`
  sections 2.2.8.6, 2.2.8.9, and 2.2.6.3.
  Folder-local named-view descriptor binaries list only real properties used by
  the visible UI columns; they must not include synthetic placeholder tags,
  table identity columns such as FolderId/MID/InstanceId/InstanceNum, or
  named-property IDs that are not resolvable in the active session. The mail
  Compact descriptor follows `[MS-OXOCFG]` section 4.2.1: Importance,
  Reminder, Icon, Flag Status, Attachment, From, Subject, Received, Size, and
  Categories are descriptor columns, while Outlook's row identity columns for
  all folder types are served only through live table `SetColumns` /
  `QueryRows`.
  The descriptor column packets follow `[MS-OXOCFG]` section 4.2 by using
  `PtypString8` / `PtypMultipleString8` for text view columns, while the
  message table projection accepts and serializes both those ANSI tags and the
  Unicode tags Outlook also asks for in live traces. KindString named-property
  columns use the portable `PropertyID = 0`, arbitrary `ID = 0x0022A764`, and
  GUID/name encoding from the `[MS-OXOCFG]` section 4.2.1.11 Categories example;
  Outlook resolves the mailbox property ID from the GUID and name.
  Sync Issues and its persisted Conflicts, Local Failures, and Server Failures
  children remain exact-ID Outlook special folders. A direct IPM-subtree table
  contains only the Sync Issues parent, while a direct Sync Issues table and a
  root `Depth` hierarchy include the three real child mailboxes and mark the
  parent as having subfolders. This keeps the Inbox-advertised failure-folder
  EntryIDs resolvable in the OST hierarchy so Outlook can move failed local
  items instead of reporting a second `MAPI_E_NOT_FOUND`. Quick Step Settings
  remains exact-ID/openable compatibility state, not an IPM subtree startup
  hierarchy row.
  Tasks, Notes, and Journal advertise `PidTagDefaultViewEntryId` through their
  type-specific Outlook view descriptors and contents row projections; To-Do
  search behavior remains bounded to the supported task/search projections.
  Delete attempts against synthetic folder-local default view objects are
  acknowledged as no-op success because the objects are
  compatibility projections, not canonical FAI messages.
- Navigation shortcut FAI rows persist in `mapi_navigation_shortcuts` for
  Outlook-created or imported Common Views shortcut messages. The bounded
  supported property surface is the visible shortcut subject, target folder
  EntryID, type, flags, save stamp, section, ordinal, group header GUID, and
  group display name. A target `PidTagWlinkEntryId` can carry either the
  account's current durable special-folder identity or an advertised legacy
  logical special-folder identity retained by the profile; the request-scoped
  decoder normalizes both to the same canonical folder role and rejects
  unadvertised logical aliases. For fresh Outlook profiles, LPE projects bounded default
  mail Favorites rows in the Common Views table/open path: the `Favorites`
  group header plus Inbox, Sent, and Trash shortcuts. These rows are an Outlook
  interoperability table projection, not a Microsoft-mandated Inbox
  view/configuration object, and they are not exported as persisted Common
  Views FAI sync changes until Outlook creates or imports durable shortcut
  rows.
  `[MS-OXOCFG]` defines navigation shortcuts as Common Views FAI messages that
  clients create, store, and later read. Outlook-created `WunderBar` group headers are
  persisted as Common Views FAI rows with `PidTagWlinkType = 4` and linked
  shortcuts retain the matching `PidTagWlinkGroupClsid`. Outlook 16.0.20228
  trace `202608111835` also creates the exact targetless Calendar group-header
  shape with raw type `5`. LPE preserves that product variant only when section
  `3`, `CLSID_CalendarFolder`, `PidTagWlinkGroupHeaderID`, and the absence of a
  target EntryID all validate; it is not a general extension of the type enum
  in `[MS-OXOCFG]` section 2.2.9.5. This scope covers
  cached-mode profile creation and reopen; full Exchange navigation-pane
  presentation parity, shared-folder shortcut semantics, public-folder shortcut
  flags, and read-only group-type extensions remain deferred until real Outlook
  traces require them.
- Outlook-created folder-associated configuration FAI messages outside Common
  Views persist in `mapi_associated_config_messages`. This table is bounded
  MAPI compatibility state for view/form/client configuration sync: it stores
  the folder id, subject, message class, and typed MAPI property bag needed for
  later associated-contents table and FAI content-sync replay. These rows are
  not canonical mailbox messages and must not be exposed through normal message
  lists, JMAP mail, IMAP, search, AI pipelines, or mailbox export as user mail.
  `PidTagRoamingDictionary` values, including the `[MS-OXOCFG]` reserved
  `OLPrefsVersion` entry, are preserved as Outlook writes them. The separately
  modeled Inbox UMOLK compatibility row carries its own bounded dictionary with
  `OLPrefsVersion = 1`, encoded as `9-1`; that server-owned projection never
  enriches or replaces an imported client's `IPM.Configuration.*` property bag.
  Inbox associated-content
  sync does not emit broad synthetic or virtual-only rows such as aggregation,
  sharing, EAS, ELC, account preferences, message-list settings, or
  extended-rule messages during broad Inbox associated-table scans. The one
  trace-backed exception is the Inbox `IPM.RuleOrganizer` FAI: Exchange 2016
  returns it from the associated-table startup query
  (`test1_202608031300.saz`, raw/551). LPE therefore enumerates the
  bounded virtual row only for that exact `PidTagMessageClass` restriction,
  with the required `Outlook Rules Organizer` subject. The matching Exchange
  `RopOpenStream` / `RopReadStream` sequence (`raw/554`) succeeds with a
  66-byte `PidTagRwRulesStream`, so the fresh virtual FAI projects that exact
  opaque default (including its leading and trailing zeroes, but excluding the
  following response-handle bytes) rather than an empty stream. In that same `RopOpenMessage`
  response, Exchange encodes the lossless ASCII normalized subject as reduced
  Unicode (String8), rather than UTF-16; LPE preserves that wire shape. Any non-empty client-owned
  `PidTagRwRulesStream` remains opaque and takes precedence over the default.
  This follows `[MS-OXORULE]` section 3.1.4.2.4. Exact,
  bounded lookups may expose persisted backed rows with valid
  payloads, and the Inbox `IPM.Configuration.UMOLK.UserOptions` exact lookup
  exposes a non-empty modeled roaming-dictionary row because Outlook 2016/2019
  startup traces abandon the Inbox normal contents table after a missing exact
  UMOLK lookup. UMOLK remains a sparse configuration FAI message: LPE returns
  real modeled or persisted properties such as `PidTagRoamingDatatypes` and
  `PidTagRoamingDictionary`, but absent optional properties are reported as
  `ecNotFound` in `RopGetPropertiesSpecific` instead of being fabricated as
  typed zero values. This applies to optional standard message properties as
  well as named properties; computed identity, change-tracking, and required
  configuration-stream properties remain available. This follows
  `[MS-OXCROPS]` section 2.2.8.3.2, `[MS-OXCDATA]` sections 2.8.1 and 2.8.1.2,
  and `[MS-OXOCFG]` configuration-data storage while leaving Outlook free to
  use or persist its own defaults.
- Reminder projection is a computed search-folder surface over canonical
  calendar/task/message data, not a protocol-local reminder store. LPE-owned
  search-folder definitions are not exported as `IPM.Microsoft.WunderBar.SFInfo`
  Common Views FAI rows until the MAPI adapter can persist and replay a
  documented `[MS-OXOSRCH]` `PidTagSearchFolderDefinition` blob. Publishing a
  locally invented SFInfo blob is an Outlook-visible protocol violation.
- `RopSetSearchCriteria` and `RopGetSearchCriteria` are bounded to canonical
  `search_folders` rows. The supported criteria subset is folder scope,
  unread/read predicates, follow-up flagged predicates, category keywords,
  attachment-presence predicates, sender display or address text, subject/body
  text, and received-date equality or inclusive bounds. Received-date criteria
  accept Outlook delivery-time restrictions and map them to canonical
  `receivedAt` JSON instead of storing an Exchange search-folder blob.
  Attachment-presence criteria accept either the bounded boolean property
  restriction or the Outlook-style existence restriction for
  `PidTagHasAttachments`; both serialize back from canonical JSON as the
  bounded property form. Category keywords serialize as
  `PidNameKeywords` `PtypMultipleString` values so accepted canonical category
  criteria remain round-trippable through `RopGetSearchCriteria`.
  `RopSetSearchCriteria` updates only existing
  user-saved search folders by translating that subset into canonical
  `scope_json` and `restriction_json` with `kind = "mapi_bounded"`.
  Built-in search folders remain read-only. Unsupported restriction operators,
  disjunctions, subobjects, comments, recipient/Bcc predicates, unknown
  folders, and any criteria that cannot round-trip through canonical JSON
  return parseable ROP-specific errors without creating a MAPI-local
  search-folder store.
- The current `[MS-OXOSRCH]` parity audit for bounded search criteria is:
  `RES_AND` over supported leaves is accepted and flattened into canonical JSON;
  `RES_CONTENT` is accepted only for subject, body, and sender text;
  `RES_PROPERTY` is accepted only for equality on read, flag status,
  attachment presence, category, sender, subject, and body plus received-date
  equality or inclusive bounds; `RES_BITMASK` is accepted only for the read bit
  in `PidTagMessageFlags`; and `RES_EXIST` is accepted only for
  `PidTagHasAttachments`. `RES_OR`, `RES_NOT`, `RES_SIZE`,
  `RES_COMPAREPROPS`, `RES_SUBRESTRICTION`, `RES_COMMENT`, `RES_COUNT`,
  recipient display predicates, Bcc-related predicates, Exchange template BLOBs,
  arbitrary Microsoft search-folder definition blobs, and malformed restriction
  blobs that leave trailing bytes after the parsed restriction remain rejected
  with parseable `RopSetSearchCriteria` / `RopGetSearchCriteria` errors until a
  canonical evaluator and serializer are explicitly documented.
- Delegate and free/busy objects are canonical projections over
  `calendar_grants`, `sender_rights`, and `calendar_events`. LPE does not create
  Exchange public-folder free/busy state or protocol-local delegate data-folder
  truth for this layer. The MAPI and EWS adapters consume the canonical
  delegate/free-busy API: same-tenant availability is exposed as computed
  free/busy blocks, calendar read grants preserve tentative/busy distinctions,
  and calendar write plus `send-on-behalf` is the supported canonical signal for
  receiving or processing meeting-related objects on behalf of a delegator.
  MAPI creates, updates, deletes, and attachment mutations against custom or
  shared calendar folders use the same canonical collection rights: read-only
  shared calendars remain visible but reject write/delete attempts without
  mutating `calendar_events` or `calendar_event_attachments`.
  LPE does not create arbitrary placeholder delegate/free-busy messages.
  The documented exception is one stable Delegate Information projection in
  Freebusy Data: `LocalFreebusy` with class
  `IPM.Microsoft.ScheduleData.FreeBusy`. Its account-scoped Message EntryID is
  the second `PidTagFreeBusyEntryIds` element on Root and Inbox, and the same
  object is openable through tables, GetProps, content sync, and direct
  FastTransfer. The object is normal content, not FAI. Its MID/SourceKey and
  version tuple are durable MAPI metadata, while its property content is a
  projection of the canonical default delegation relationship and never a
  second delegate-data source of truth. This follows `[MS-OXOSFLD]` section
  2.2.6, `[MS-OXOPFFB]` section 4.2, and `[MS-OXODLGT]` sections 2.2.2.1.1,
  2.2.2.1.2, 2.2.2.2, and 3.1.4.3.4.
- `PidTagSwappedToDoData` uses the documented version-1 validation. Malformed
  blobs fail validation instead of being accepted into canonical task state.
- Journal and Notes data are canonical account-owned items. MAPI coverage must
  project and mutate them only through canonical item tables, APIs, and change
  tracking.

### Transport Spooler Advisory ROPs

`RopSetSpooler`, `RopSpoolerLockMessage`, and `RopTransportNewMail` remain
parseable unsupported probes until LPE has a canonical advisory model that is
observable outside the MAPI session. The current canonical transport state is
`submission_queue`, `submission_recipients`, `submission_events`,
`mail_change_log`, and the LPE-to-LPE-CT handoff. None of those tables expresses
client-spooler ownership, per-message spooler locks, or client-announced new
mail delivery.

The supported design constraints are:

- `RopSetSpooler` must not persist a MAPI-local "spooler active" flag. If a
  later Outlook trace requires an acknowledgement, it can become a session-local
  no-op only after tests prove Outlook does not depend on durable behavior.
- `RopSpoolerLockMessage` must not lock canonical messages or queue rows because
  LPE-CT owns transport custody after handoff and canonical mailbox state uses
  normal transaction boundaries. A future implementation needs a documented
  queue lease or advisory lock model shared with the outbound worker before this
  ROP can mutate state.
- `RopTransportNewMail` must not create or announce inbound mail. Inbound
  delivery belongs to LPE-CT final delivery and canonical mailbox insertion; MAPI
  clients learn about new mail through contents sync, notifications, and
  `mail_change_log` replay.

Until those prerequisites exist, all three ROPs are parsed to their documented
request lengths and return ROP-specific protocol errors without modifying
mailbox, submission, notification, or LPE-CT state.

The transport-folder, spooler-advisory, abort-submit, and store-state ROPs that
are defined on a private Store object require an actual private Logon handle.
A live Message, Folder, table, public-folder Logon, or other server object is
not coerced into that Store role and cannot cancel a queued submission. This
follows `[MS-OXOMSG]` sections 2.2.5.1.1, 2.2.5.2.1, 2.2.5.3.1, 2.2.5.5.1,
and 3.3.5.2 and `[MS-OXCSTOR]` sections 2.2.1.5.1 and 3.2.5.5.

The transport-folder, spooler-advisory, abort-submit, and store-state ROPs that
are defined on a private Store object require an actual private Logon handle.
A live Message, Folder, table, public-folder Logon, or other server object is
not coerced into that Store role and cannot cancel a queued submission. This
follows `[MS-OXOMSG]` sections 2.2.5.1.1, 2.2.5.2.1, 2.2.5.3.1, 2.2.5.5.1,
and 3.3.5.2 and `[MS-OXCSTOR]` sections 2.2.1.5.1 and 3.2.5.5.

## Implemented Coverage

The implemented coverage described here is the guarded local surface and does
not by itself authorize broad client publication.

### Transport and Bootstrap

- Authenticated MAPI over HTTP endpoint routing exists for the bounded EMSMDB
  and NSPI surfaces.
- EMSMDB session context handling covers connection, execution, disconnect,
  request id handling, client info echoing, response-code mapping, cookies, and
  overlapping same-session sequence validation. Password and app-password
  verification plus successful-login audit happen once per established or
  reconnected EMSMDB/NSPI context; accepted continuation requests use the
  bound session proof plus a current lightweight verifier-state check.
- Profile bootstrap projects private mailbox store/logon properties, default
  folder identities, hierarchy metadata, and basic contents sync data required
  by the local Outlook cached-mode gate.

### EMSMDB ROP Coverage

- Store and folder open paths cover the private mailbox root, IPM subtree,
  default folders, contents tables, hierarchy tables, attachment tables, and
  permission table projection.
- The current ROP surface includes bounded support for property reads/writes,
  table query, hierarchy sync, content sync, FastTransfer source buffering,
  message import/save, draft/send flows, read-state changes, deletes, whole-folder
  canonical mailbox content purges, moves, copies, and attachment reads/writes
  where backed by canonical state.
- Attachment projection follows `[MS-OXCMSG]` section 4.4 for inline HTML image
  metadata: `PidTagAttachContentId`, `PidTagAttachFlags`, and
  `PidTagAttachmentHidden` are preserved across create/save, attachment table
  reads, attachment property reads, and draft submission into canonical LPE state.
- Outlook's `PidTagAdditionalRenEntryIds` multi-binary special-folder cache is
  accepted as Inbox metadata during cached-mode bootstrap. `[MS-OXOSFLD]`
  section 2.2.4 defines the five documented positions and requires opaque later
  positions to be preserved. The Exchange 2016 reference has a successful
  `RopGetPropertiesSpecific` read with five canonical folder EntryIDs and an
  opaque sixth value; it does not capture an `AdditionalRenEntryIds` write. The
  LPE Outlook trace shows the abbreviated client write. LPE therefore begins an
  Inbox write with its previous profile value (or the canonical five documented
  entries), rewrites documented
  positions 0 through 4 to the canonical values, and preserves only omitted or
  supplied opaque later positions. This lets Outlook's four-entry prefix retain
  canonical Junk E-mail at index 4 and an existing client Junk move stamp at
  index 5. The bounded merged value persists in
  `mapi_folder_profile_property_values`; it does not create canonical folder
  truth. A recognized alternate FID submitted in a documented index is retained
  separately as bounded account-scoped protocol identity metadata in
  `mapi_special_folder_aliases`, but it is never re-advertised through that
  property. It remains an LPE-only open redirect for a stale cached request
  after a new EMSMDB session or server restart. Multiple profiles or OST replicas
  may contribute different aliases for one canonical FID; an alias FID or
  SourceKey can map to only one canonical FID. An alias never overrides computed
  canonical default-folder properties or becomes a second visible hierarchy
  identity when that canonical folder is in the configured full hierarchy
  projection. An alias is not an LPE hierarchy replica folder object. Unlearned
  client-local folder identifiers remain unmapped and fail through the normal
  `ecNotFound` folder-open path. Profile bytes, aliases, the Inbox hierarchy
  version tuple, and its replay row are one PostgreSQL commit; hierarchy import
  normalizes the documented indexes before applying the same transaction, so
  an empty client prefix cannot erase the canonical special-folder identities.
- Outlook scalar default-folder EntryID writebacks on Root or Inbox are validated
  against the canonical special-folder map and acknowledged for interoperability.
  A valid alternate FID and matching SourceKey are recorded through the same
  durable account-scoped alias path, but they never override the canonical
  projection or create a second folder or user-visible content record.
- Store-level `RopGetPropertiesAll` and `RopGetPropertiesList` enumerate the
  same computed default-folder identities as targeted store `GetProps` calls,
  including `PidTagIpmAppointmentEntryId`, so Outlook bootstrap paths that
  discover Calendar through broad store-property enumeration receive the
  canonical Calendar EntryID without relying on MAPI-local folder state.
- `RopGetReceiveFolder`, `RopGetReceiveFolderTable`, and bounded
  `RopSetReceiveFolder` use the same canonical receive-folder map. The only
  accepted `IPM.Appointment` or `IPM.Appointment.*` receive-folder write is the
  canonical Calendar folder; mismatched writes are rejected without creating
  protocol-local receive-folder state.
- Root and Inbox `RopGetPropertiesAll` / `RopGetPropertiesList` enumerate the
  same computed default-folder identity properties for Outlook's documented
  Inbox-first, Root-fallback special-folder discovery path; the values remain
  computed from canonical reserved MAPI folder identities.
- `PidTagValidFolderMask` is kept aligned with the special-folder EntryIDs LPE
  advertises for the documented store-level mask surface, including Finder /
  Search.
- Outlook store bootstrap metadata includes the private-store marker, store
  state, mailbox owner, user GUID, minimal valid server icon payloads, and max
  submit message size.
- Profile settings needed for cached-mode reuse are canonical account settings,
  not session-only state. Outlook's IPM subtree OST identity value
  (`0x7C04_0102` in the current bounded profile path) is persisted in
  `mapi_profile_settings.ipm_subtree_ost_id` when Outlook writes it to the IPM
  subtree and is reloaded when the folder is opened in a later session. The
  stored value remains a bounded profile setting, with a 2048-byte limit that
  covers observed Outlook cached-mode values without becoming a general OST
  profile store. If the persistence path is unavailable, the accepted write
  remains visible in the current session and logs the persistence failure so
  Outlook bootstrap can continue; installation checks must report the missing
  canonical schema state.
- `RopGetReceiveFolder` and `RopGetReceiveFolderTable` use the same primed
  receive-folder table: `IPM` and `IPM.Note` resolve to Inbox and
  `IPM.Appointment` resolves to the canonical Calendar folder. Empty or
  unmatched message-class probes return Inbox with an empty explicit class,
  matching the documented `RopGetReceiveFolder` longest-prefix fallback.
- Calendar RCA diagnostics log the `PR_IPM_APPOINTMENT_ENTRYID` folder EntryID,
  decoded Calendar FID, `IPF.Appointment` folder contract, default calendar
  collection presence, projected event count, and effective access state when
  Outlook opens the Calendar folder. The log must distinguish a truly wired
  canonical Calendar projection from an advertised special-folder shell.
- Unsupported or malformed ROPs use parseable error responses and terminate the
  current buffer as required by the wire contract.

### NSPI Coverage

- NSPI can resolve the authenticated mailbox and canonical visible contacts for
  Outlook address-book bootstrap.
- NSPI projects `PidTagAddressBookObjectGuid` as the Windows GUID byte layout
  expected by Outlook address book clients.
- NSPI hierarchy rows use object Minimal Entry IDs outside the reserved
  `0x00000000` through `0x0000000F` signal range. `DNToMId` parses its own
  ASCII DN array up to the 100,000-value protocol bound and preserves order,
  duplicates, and one-to-one cardinality; an organization DN that has no
  canonical LPE object maps to `0`, never to the authenticated mailbox
  (MS-OXNSPI sections 2.2.1.8, 2.2.7.1, 2.2.9.1, and 3.1.4.1.13;
  MS-OXCMAPIHTTP sections 2.2.5.4.1 and 2.2.5.4.2).
- NSPI `GetProps` preserves the requested property order, duplicates, null
  placeholders, and response code page. An unavailable property is returned in
  its original slot as `PtypErrorCode` with `ErrorsReturned`, including when
  `CurrentRec` does not identify an address-book object, rather than being
  dropped or replaced by bootstrap columns (MS-OXNSPI section 3.1.4.1.7;
  MS-OXCMAPIHTTP sections 2.2.5.7.1 and 2.2.5.7.2; MS-OXCDATA section 2.11.1).
- `PidTagDisplayTypeEx` uses the canonical address-book display type in its
  local-display byte (MS-OXOABK section 2.2.3.12).
- NSPI mutation and advanced link-table operations are intentionally deferred.

### ICS and FastTransfer Coverage

- `RopSynchronizationOpenCollector.IsContentsCollector` is decoded as the
  one-byte, 8-bit MAPI Boolean defined by
  [[MS-OXCDATA] section 2.11.1](https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcdata/0c77892e-288e-435a-9c49-be1c20c7afdb): raw `0` opens
  a hierarchy collector and raw `1` opens a contents collector. LPE does not
  reinterpret raw `0` as the download-only `SynchronizationType` value
  `Contents (0x01)`. This follows
  [[MS-OXCFXICS] section 2.2.3.2.4.1.1](https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcfxics/7fa21d88-71ba-4cfe-8af5-4d7902489e5f)
  and
  [[MS-OXCROPS] section 2.2.13.7.1](https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcrops/8588e7dd-4732-4d3e-bf28-82bbdf0b4d04).
- Hierarchy synchronization emits canonical folder identities, source keys,
  change keys, predecessor lists, special-folder fields, content counts, unread
  counts, `PidTagLocalCommitTimeMax`, `PidTagDeletedCountTotal`, and final state.
  Folder-change rows include `PidTagFolderId` when the client requests the `Eid`
  synchronization extra flag; Outlook's cached-mode hierarchy request does so,
  which lets it bind default folders such as Calendar.
  `MetaTagIdsetGiven` is sent as property tag `0x40170003` while its payload is
  serialized as binary, matching the Microsoft ICS state compatibility rule.
  Every reserved private-store folder also has a durable
  `(CN, ChangeKey, PCL, LMT)` version tuple in `mapi_object_identities`; its
  FID/SourceKey remain the distinct object identity. The hierarchy import
  validates all six fixed properties, allocates a server CN rather than reusing
  the FID, retains the accepted imported ChangeKey, merges the imported and
  current PCL lineages, and persists the winning last-modification time. A
  non-conflicting `Applied` result and a `Conflict` result each allocate a
  successor CN and write one `mail_change_log` row marked `mapiOnly`. That row
  drives durable MAPI hierarchy delta and notification replay but is filtered
  out of JMAP `Mailbox/changes`. Only `Applied` adds the new CN to the upload
  `MetaTagCnsetSeen`; `Conflict` returns `Success` without that addition so the
  client downloads the resolved server version. An exact `Duplicate` returns
  `IgnoreFailure` without allocating a CN or writing a change-log row. Conflict
  resolution merges both PCLs and selects ChangeKey/LMT by last-writer-wins;
  equal timestamps retain the current server winner. The same durable tuple is
  projected in the importing Execute and after reconnect. `PidTagHierRev` and
  `PidTagLocalCommitTime` use its LMT; `PidTagLocalCommitTimeMax` remains the
  separate most-recent change time of a top-level child. This follows
  `[MS-OXCFXICS]` sections 2.2.1.2.3, 2.2.1.2.7, 2.2.1.2.8,
  2.2.3.2.4.3.1, 3.1.5.3, 3.1.5.6.2.2, and 3.2.5.9.4.3 and
  `[MS-OXCFOLD]` sections 2.2.2.2.1.9, 2.2.2.2.1.13, and
  2.2.2.2.1.14.
- Contents synchronization emits canonical message-change rows, folder-associated
  information rows for the bounded bootstrap surface, canonical conversation
  action FAI rows, destroyed conversation actions as `IncrSyncDel`, tombstones,
  read-state changes, and final state. It emits no conversation action row when
  canonical `conversation_actions` is empty.
- Normal-mail content rows retain their persisted `mapi_object_identities`
  identity/version tuple when a snapshot is loaded: `PidTagMid`, SourceKey,
  ChangeNumber, ChangeKey, predecessor list, and last-modification time are not
  synthesized from mailbox `modseq`. A canonical normal-mail content,
  recipient, attachment, non-read flag, or existing-draft mutation preserves
  its MID, SourceKey, and InstanceKey while atomically allocating and persisting
  a successor CN/ChangeKey/PCL. The same durable CN is used for the emitted row
  and `MetaTagCnsetSeen`, so a client cannot acknowledge a synthetic version
  that differs from the advertised object. This follows `[MS-OXCFXICS]`
  sections 2.2.1.2.3, 2.2.1.2.7, 2.2.1.2.8, 3.1.5.3, and 3.2.5.9.1.1.
- Normal-message direct GetProps, regular/categorized contents tables, and
  FindRow use that same durable tuple. Direct `CopyTo`/`CopyProperties` project
  SourceKey, ChangeKey, predecessor list, and last-modification time only when
  the existing property filter permits them; provider-internal MID and CN remain
  outside that direct message-content projection. This keeps the direct and ICS
  versions coherent without changing the documented CopyTo/CopyProperties
  selection contract.
- A server-side normal-mail inter-folder move allocates a distinct destination
  MID and SourceKey, retains the retired source MID on the source
  mailbox-message tombstone, and emits that exact retired MID in the source
  ICS deletion. Its moved notification therefore carries distinct destination
  and old message identifiers rather than reusing the destination MID. This
  follows `[MS-OXCFXICS]` section 3.1.5.3 and `[MS-OXCNOTIF]` section
  2.2.1.4.1.2.
- When `SynchronizationExtraFlags.OrderByDeliveryTime` is set, the complete
  normal/FAI `messageChange` sequence is ordered newest to oldest by
  `PidTagMessageDeliveryTime`, falling back to `PidTagLastModificationTime` when
  delivery time is absent. This follows `[MS-OXCFXICS]` section 3.2.5.9.1.1,
  `[MS-OXOMSG]` section 2.2.3.9, and `[MS-OXPROPS]` section 2.766.
- A Calendar `RopSynchronizationImportMessageChange` creates a pending canonical
  Event. Its following property/stream writes and `RopSaveChangesMessage`
  preserve the imported SourceKey and client-reserved MID, allocate a distinct
  internal server CN, and retain the imported ChangeKey/PCL/LMT as the current
  version. The new CN advances `MetaTagCnsetSeen` without replacing that
  foreign ChangeKey or LMT. This is the `[MS-OXCFXICS]` sections 2.2.3.2.4.2.1,
  3.1.5.3, and 3.2.5.9.4.2 contract.
  It must never fall through to generic mail persistence for an
  `IPM.Appointment`.
- The same import ROP for an existing active or deleted Event returns a writable
  handle for that canonical Event. The imported SourceKey has to match its
  current identity; the following property writes and Save update the same
  Event and preserve its MID/SourceKey while allocating a distinct server CN.
  A non-conflicting or client-winning update keeps the imported ChangeKey and
  LMT; a server-winning conflict keeps the current server ChangeKey and LMT,
  and either accepted conflict commits the merged predecessor lineage. This includes
  Outlook's required move-then-modify sequence and is atomic through the parent Save; invalid
  identity material leaves the Event unchanged. This follows `[MS-OXCFXICS]`
  sections 3.1.5.3, 3.3.4.3.3.2.1.1, and 3.3.4.3.3.2.2.1 and
  `[MS-OXCMSG]` sections 2.2.3.3.1 and 3.2.5.3.
- Associated/FAI Save likewise retains the imported ChangeKey. Exchange controls
  `logs/test1_202608031300.saz` raw 306->307, 467->468, 513->514, and
  689->690 preserve the imported associated ChangeKey on initial creation and
  later updates, consistent with the general ICS imported-version rule.
- Existing-Event imports compare the incoming and current PCLs as specified by
  `[MS-OXCFXICS]` section 3.1.5.6.1. An older or equal client version is
  acknowledged without mutating the Event. A conflict with `FailOnConflict`
  returns `SyncConflict (0x80040802)`; an accepted conflict merges both PCLs
  and applies the section 3.1.5.6.2.2 last-writer-wins rule. The resulting PCL
  is a successor of both versions as required by sections 3.1.5.6.2 and
  3.2.5.9.4.2.
- After a new or changed Calendar import is saved, its upload collector unions
  the Event's distinct server CN into `MetaTagCnsetSeen`; it does not add that
  CN to `MetaTagCnsetSeenFAI` or `MetaTagCnsetRead`, and the upload collector
  never returns `MetaTagIdsetGiven`. The client advances its local Given set
  after a successful import. This follows `[MS-OXCFXICS]` sections 3.1.5.2.1,
  3.1.5.3, 3.2.5.2.1, and 3.3.5.8.7.
- Content and hierarchy manifests are selected from canonical folder membership
  and canonical change tracking rather than from primary mailbox fields alone.
- FastTransfer source buffering emits parseable transfer chunks and validates
  strict ICS/FastTransfer value encoding. Message-object CopyTo/CopyProperties
  buffering starts with the first `messageContent` property and contains no
  outer message marker. `RopFastTransferSourceCopyFolder` accepts only a live
  Folder object; a Message object's containing-folder lineage never authorizes
  folder-copy serialization (`[MS-OXCFXICS]` section 2.2.3.1.1.4.1).
- `RopTellVersion` accepts only a genuine FastTransfer download or upload
  context. An ICS download `SynchronizationSource` or upload
  `SynchronizationCollector` returns `ecNotSupported` without changing that
  context, so later ROPs in the same buffer remain aligned and usable. This
  follows the operation-applicability table and FastTransfer sequencing in
  `[MS-OXCFXICS]` sections 2.2.3, 2.2.3.1.1.6, 3.3.4.1, and 3.3.4.2.
- Property `RopCopyTo` and `RopCopyProperties` require compatible live
  Message, Folder, or Attachment object families before an empty property list
  can report success or a custom value can be copied. Incompatible live object
  families return `ecNotSupported` before mutation, following `[MS-OXCPRPT]`
  sections 2.2.10, 2.2.11, and 3.2.5.8.

### Canonical Projection Coverage

- Search folder and reminder bootstrap projection is backed by canonical
  persisted definitions and bounded evaluators. User-saved Search Folders are
  synchronized as canonical folder definitions through hierarchy tables and
  hierarchy sync; full arbitrary Exchange search-result materialization remains
  deferred until the canonical restriction evaluator is widened.
- Conversation action FAI rows and destroyed conversation actions are projected
  for the supported cached-mode sync path.
- Notes and Journal item projection uses canonical item state and must remain
  aligned with canonical API behavior.
- Session-scoped notification support can mark content and hierarchy changes as
  pending, include bounded TableModified-style payloads with the changed folder
  ID, changed message/object ID, canonical change cursor, modseq, folder counts,
  object/change kind, display names, and message subject when those values are
  available from canonical `mail_change_log` replay. NewMail payloads carry the
  event's MessageClass after the Unicode flag, with `IPM.Note` only as the
  compatibility fallback for a class-less legacy event, as specified by
  `[MS-OXCNOTIF]` section 2.2.1.4.1.2. Message `ObjectMoved` and
  `ObjectCopied` data is emitted only when replay resolves both destination
  `FolderId`/`MessageId` and source `OldFolderId`/`OldMessageId`: a stable-ID
  move snapshots its equal MID pair, while an imported rekey snapshots both
  IDs before replacing the active identity. An incomplete
  movement notification is suppressed rather than guessing a source ID or
  substituting a `TableModified` notification; a separately subscribed table event and
  ordinary ICS remain independent convergence paths. An active table without
  an explicit registration initializes the session-local durable notification
  cursor before the containing Execute's mail-store snapshot and adopts it when
  the compatible table becomes active, allowing its automatic subscription to
  replay a canonical change that arrives during that request; a
  `NoNotifications` (`0x10`) table is excluded. A hierarchy table opened with
  `SuppressesNotifications` (`0x80`) remains automatically subscribed to
  external changes, but an automatic table notification caused by a ROP in the
  same client Execute is suppressed; an explicit `RopRegisterNotification`
  subscription remains independent and can still receive that event. The
  Execute captures sparse direct mutation events before polling the durable
  change log, correlates a richer polled echo by event kind, folder, stable
  canonical/message identity, and modseq when both sides provide one, and then
  discards the origin set. It therefore cannot suppress a later external
  change. This boundary matches the LPE `202608041041.saz` interoperability capture:
  raw 147/151 kept a root `0x84` hierarchy table live and queried through raw
  221, while the same `MapiContext` message save changed a folder count without
  emitting a notification for that table. Registrations,
  automatic table subscriptions, and pending event delivery remain session-local;
  after process restart or movement to a different worker, the session must
  re-register and resume from canonical sync/checkpoint behavior rather than
  relying on cross-process notification delivery. This implements the automatic
  subscription behavior in [MS-OXCNOTIF] section 3.1.4.3 and the table flags in
  [MS-OXCFOLD] sections 2.2.1.13.1 and 2.2.1.14.1. Notification FolderId and MessageId fields are serialized through
  the authenticated request's scoped identity codec, including an otherwise
  release-only Execute that has registered notification targets; logical
  default-folder role IDs are never emitted as wire identifiers. An active,
  unrestricted hierarchy table with a prior `RopSetColumns` receives an
  informative `TableRowModified` payload when a child-content change leaves
  its containing folder row in the table's current non-count sort order; the
  payload uses its existing column projection, current aggregate values, and
  predecessor folder ID; separately changed folder rows are not coalesced. A
  row refresh caused by a message or collaboration-item change retains the
  `M` notification flag and its zero message/instance key fields; a NewMail
  cause additionally retains the Exchange search-folder flag. This follows
  `[MS-OXCNOTIF]` section 2.2.1.4.1.2 and preserves the Exchange hierarchy-row
  wire shape rather than collapsing every cause to a folder-only `0x0100`
  notification.
  Explicit
  subscription deliveries, including `NewMail`, are emitted before automatic
  table notifications from the same canonical change. Restricted hierarchy
  tables and tables sorted by changing counts retain the basic `TableChanged`
  fallback. The generic mailbox-copy path does not yet produce a durable
  `copied` change, so it cannot emit `ObjectCopied` until the canonical copy
  identity lifecycle is implemented. Full notification registration, all table
  row values for every view shape, and Exchange delivery parity remain deferred.

## Deferred Surfaces

| Surface | Status |
| --- | --- |
| Public folders | Public-folder logon, hierarchy/content projections, post create/update/delete/copy/move, ACL read/write, read-state ROPs, bounded LPE-owned per-user information stream round-trip, canonical replica topology projection through `RopGetOwningServers`, and `RopPublicFolderIsGhosted` ghost-state derivation are implemented over the canonical public-folder layer documented in `docs/architecture/public-folders-mapi-mvp.md`. Exchange-compatible cross-server public-folder replication, recipient-bearing item conversion, and arbitrary Exchange-compatible per-user binary blobs remain deferred and must return parseable errors without creating protocol-local public-folder state. |
| Outlook Anywhere / RPC over HTTP | Deferred legacy compatibility shim. `EXPR` publication requires a real `/rpc/rpcproxy.dll` path and separate evidence. |
| Cross-process MAPI session replay and load-balanced failover | Deferred production hardening. First lab gate may use single-node sticky sessions. |
| Client SMTP in core LPE | Forbidden. Submission must use canonical LPE submission, not a client SMTP endpoint in the core server. |
| Protocol-local Sent/Outbox | Forbidden. Sent and submission state must be canonical. |
| NSPI mutation | Deferred. Address-book writes and link-table mutation remain disabled. |
| Raw FastTransfer destination upload streams | Partially implemented. Destination configure plus `RopFastTransferDestinationPutBuffer` and `RopFastTransferDestinationPutBufferExtended` accept sequential bounded property-stream buffers when request boundaries fall between MS-OXCFXICS lexical elements, and also retain an incomplete suffix when a length-delimited `varSizeValue` continues in the next request. This does not claim support for a request boundary inside a fixed-width atom or `namedPropInfo`. The decoder reads `namedPropInfo` for every wire property ID greater than or equal to `0x8000`, including IDs greater than or equal to `0xC000`; it resolves a known mapping or durably allocates the destination mailbox mapping, normalizes well-known Calendar aliases, and supports `ServerId` plus multiple-String8 and multiple-Unicode framing. Each ROP returns the complete MS-OXCROPS success or failure response shape, including the request's `InputHandleIndex` and the accepted `BufferUsedSize`. Calendar RSVP fields are an aggregate invariant, so per-chunk decode stages the individually valid fields and defers cross-field RSVP validation until the complete property set is applied or the pending object is saved. Complete properties are routed through the existing canonical save/import path; Exchange marker/subobject stream shapes remain unimplemented and return parseable ROP errors without creating protocol-local state. This follows `[MS-OXCFXICS]` sections 2.2.3.1.2.2, 2.2.3.1.2.3, 2.2.4.1-2.2.4.1.4, 3.2.5.8.2.2, and 3.2.5.8.2.3; `[MS-OXCROPS]` sections 2.2.12.2-2.2.12.3.2; and `[MS-OXCDATA]` section 2.11.1.4. |
| Non-mailbox recursive purge | Deferred until canonical folder lifecycle semantics and interoperability evidence are complete. `RopEmptyFolder` is bounded to hard-deleting visible memberships in the target canonical mailbox folder through the canonical tombstone/change-log path. `RopHardDeleteMessagesAndSubfolders` recurses only through canonical mailbox descendants and does not delete non-mailbox objects. Public-folder whole-folder purge returns a parseable not-supported ROP error; public-folder item delete/move/copy remains item-scoped through canonical public-folder APIs. |
| Recoverable Items / dumpster ROP exposure | Bounded MAPI Recoverable Items Root, Deletions, Versions, and Purges virtual folders project canonical `recoverable_items` lifecycle state for browse, restore, and purge only. `RopMoveCopyMessages` move from a concrete recoverable subfolder uses canonical recoverable restore, `RopMoveCopyMessages` copy returns a parseable not-supported error, `RopDeleteMessages` on recoverable folders returns partial completion without purging because LPE does not yet implement Exchange's Deletions-to-Purges soft-delete progression, purge and empty-folder on Deletions, Versions, or Purges use canonical recoverable purge, Recoverable Items Root message mutation and purge calls return parseable not-supported errors, retention/legal-hold failures return partial completion, and recovery state stays out of normal mailbox hierarchy/content sync. `RopGetContentsTable` with `SoftDeletes` (`0x20`) returns a parseable not-supported ROP error because canonical LPE hard delete/Trash purge removes normal folder membership and writes `recoverable_items` rows instead of keeping folder-local soft-deleted rows. `OpenSoftDeleted` and complete Exchange dumpster folder parity remain gated on canonical lifecycle semantics; any MAPI-local dumpster store is forbidden. Versions and Purges are bounded virtual projections over canonical lifecycle rows; LPE does not claim Exchange copy-on-write Versions behavior or full Purges post-recovery parity. |
| Sync move import | `RopSynchronizationImportMessageMove` parses all five documented length-prefixed fields as GID/XID/PCL values. For an Outlook optimizing-send move, it finds the transient same-message canonical Outbox membership created from `PidTagTargetEntryId`, removes that source membership, and atomically rekeys the active MAPI identity to the imported destination SourceKey, ChangeKey, and PCL while allocating a distinct internal server ChangeNumber; the already-canonical Sent item remains the one user-visible sent message and no full content re-upload is required. An exact retry after response loss is idempotently acknowledged when that destination identity and target membership remain active while the source membership is absent, and reannounces the target change without another mutation. A no-conflict Calendar-to-Deleted-Items import moves the canonical Event to its deleted lifecycle and performs the corresponding principal-identity rekey; it does not create a generic `IPM.Appointment` mail row. This follows `[MS-OXCFXICS]` sections 2.2.3.2.4.4.1, 3.1.5.3, 3.2.5.9.4.4, 3.3.4.3.3.2.1.1, and 3.3.4.3.3.2.1.2; `[MS-OXOMSG]` sections 3.2.4.4 and 3.3.5.1.3; and `[MS-OXCROPS]` sections 2.2.13.6.1-2.2.13.6.3. Concurrent-move conflict handling and the `NewerClientChange` (`0x00040821`) response remain a separate interoperability gate. |
| Sync hierarchy import | `RopSynchronizationImportHierarchyChange` validates the six fixed hierarchy properties before routing by SourceKey/FID. It applies the durable tuple transition above to an existing canonical reserved folder. An ordinary system-folder alias import persists only account-scoped `(alias FID, SourceKey, server CN) -> canonical FID` protocol identity metadata in `mapi_special_folder_aliases`; it does not create a shadow mailbox, Calendar, Contacts folder, or user-visible row. An alias-form Inbox import carrying `PidTagAdditionalRenEntryIds` is the bounded exception: the validated alias, normalized profile-property patch, and Inbox CK/PCL/LMT/CN/journal transition use the same atomic Inbox transaction. The client FID must use `REPLID 1`, its GLOBCNT must be inside a range previously reserved for that account by `RopGetLocalReplicaIds`, and the 22-byte SourceKey must contain the store replica GUID plus the same GLOBCNT. Canonical special-folder FIDs use `REPLID 1` and reserved GLOBCNT values `1..42`; persistable aliases are bounded to `43 <= GLOBCNT < 0x7FFF_FE00_0000`. Alias FID and SourceKey collisions with `mapi_object_identities` are rejected. Multiple alias records may share one canonical FID. Imported parent SourceKeys resolve through both canonical identities and these aliases. Successful alias import always adds its server CN to upload `MetaTagCnsetSeen`, and the imported alias FID remains resident in the originating client's hierarchy `MetaTagIdsetGiven`, following `[MS-OXCFXICS]` sections 3.2.5.9.4.3 and 3.3.5.8.8. The alias remains a durable redirect and is not emitted as a second hierarchy row when the canonical target is already projected. Upload `MetaTagIdsetGiven` is ignored rather than echoed; the later download selection retains only aliases already present in that client's state. Atomically preserving an imported CK/PCL/LMT tuple during first-time custom-folder creation, existing custom-folder rename/move, ordinary system-folder alias rename/move, and advancing the tuple for canonical folder mutations performed outside this import path are not implemented yet and remain required Outlook interoperability work. This follows [MS-OXCFXICS] sections 2.2.1.2.3, 2.2.1.2.7, 2.2.1.2.8, [2.2.3.2.4.3.1](https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcfxics/9d5d9d68-775d-4ede-a14c-119bc54a6327), 2.2.3.2.4.7.2, 3.1.5.3, 3.1.5.6.2.2, 3.2.5.9.4.3, 3.3.5.2.1, and 3.3.5.8.12, plus [MS-OXCFXICS section 3.3.5.8.8](https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcfxics/f3fab904-bb7d-4cf3-bbd6-65d4a34b67d2) and [MS-OXCFOLD] sections 2.2.2.2.1.9, 2.2.2.2.1.13, and 2.2.2.2.1.14. |
| Full search-folder parity | Partially implemented. Bounded `RopSetSearchCriteria` / `RopGetSearchCriteria` support exists only for canonical `mapi_bounded` JSON over folder scope, unread, flagged, attachment presence including `PidTagHasAttachments` existence probes, `PidNameKeywords` category property equality, sender, subject/body text, and received-date bounds. Full Microsoft template BLOB parity, arbitrary restriction trees, recipient/Bcc predicates, and secondary sender/recipient reminder promotion remain deferred. |
| Rules and deferred actions | Partially implemented. `RopGetRulesTable` projects canonical Sieve-backed mailbox rules for Outlook profile visibility. Bounded `RopModifyRules` support writes only generated canonical Sieve rules for cleanly mapped move/delete/mark-read/forward/redirect/stop-processing mutations. Exchange rule blobs, client-only rules, provider-specific predicates, delegate rule templates, deferred-action provider data, and `RopUpdateDeferredActionMessages` are not implemented yet because their canonical rule/deferred-action model is still missing; no MAPI-local rule store is allowed and rejected deferred actions do not activate Sieve. |
| Folder permission mutation | Partially implemented. `RopModifyPermissions` maps bounded same-tenant account ACL rows to canonical `mailbox_delegation_grants` for mail folders and canonical `calendar_grants` for default, owned custom, and share-right delegated calendar folders, with audit and change-log writes. The fresh Freebusy Data table accepts Outlook's empty `ReplaceRows` request. Nonempty Freebusy ACL copying remains unsupported because the current canonical rights tuple cannot preserve Calendar Author versus Editor and the distinct Freebusy Editor role; it must not silently widen rights or perform a non-atomic replacement. Exchange-only ACL subjects remain gated on canonical principal semantics, and MAPI-local ACL storage is forbidden. |
| Full notification registration and delivery | Partially implemented through session-local pending events with bounded folder/message/table payloads and durable canonical or MAPI-only change-cursor replay across storage/server instances. Registration handles, pending queues, and their ownership remain session-local; clients must re-register after reconnect or worker movement and use normal sync to converge. |
| Outlook tolerance beyond the documented lab matrix | Unknown until captured through the release gates below. |

## Outlook Server-Side Profile Data Matrix

| Profile data | Canonical storage | API | JMAP | MAPI over HTTP | Tests and gaps |
| --- | --- | --- | --- | --- | --- |
| Messages | `messages`, `mailbox_messages`, `recoverable_items`, MIME/body/blob tables, submission rows | `/api/mail/messages/submit`, draft and flag APIs; `/api/mail/recoverable-items` browse/restore/purge | `Email/*`, `Mailbox/*`, `Thread/*`, `EmailSubmission/*`; normal views exclude recoverable items | Contents tables, ICS, FastTransfer, import/save/send ROPs; bounded Recoverable Items virtual folders for browse/restore/purge over `recoverable_items` | Covered by existing mail/JMAP/MAPI tests plus canonical recoverable-state tests; no PST/OST content handling. |
| Contacts | collaboration contact collections and contact rows | `/api/mail/contacts` and sharing APIs | `AddressBook/*`, `ContactCard/*` | NSPI and MAPI contact projections; contacts with canonical Email1-Email3 values project synchronized `PidLidAddressBookProviderEmailList` and `PidLidAddressBookProviderArrayType` values as required by `[MS-OXOCNTC]` sections 2.2.1.2, 2.2.1.2.11, and 2.2.1.2.12 | Covered by collaboration/JMAP/MAPI tests; NSPI mutation remains deferred. |
| Calendars | calendar collections, events, grants, free/busy projections | `/api/mail/calendar/events`, delegation/free-busy APIs | `Calendar/*`, `CalendarEvent/*` | Calendar folder, appointment EntryIDs, free/busy/delegate projections | Covered by calendar/JMAP/MAPI tests; full Exchange delegate data folders remain gated on canonical delegate/free-busy semantics. |
| Tasks | task lists, task rows, grants, reminder metadata | `/api/mail/tasks`, `/api/mail/task-lists`, reminders API | `TaskList/*`, `Task/*`, `Reminder/*` | Task folder and reminder/search-folder projections | Covered by task/reminder/JMAP/MAPI tests. |
| Notes | canonical client note rows | `/api/mail/notes` | private `Note/*` | Notes folder item projection and custom properties | Covered by notes API/JMAP/MAPI tests. |
| Journals | canonical journal rows | `/api/mail/journal` | private `JournalEntry/*` | Journal folder item projection and custom properties | Covered by journal API/JMAP/MAPI tests. |
| Search Folders | `search_folders` definitions plus hierarchy/content projections | `/api/mail/search-folders` | private `SearchFolder/*` | `FOLDER_SEARCH` hierarchy rows and bounded evaluators; no Common Views SFInfo rows until `[MS-OXOSRCH]` BLOB parity exists | CRUD and projection tests cover canonical wiring; full Microsoft search template BLOB parity remains deferred. |
| Rules | `sieve_scripts` | `/api/mail/rules` read projection; Sieve API mutates | private read-only `Rule/*` | `RopGetRulesTable` projection plus bounded generated-Sieve `RopModifyRules` mutations | Persistence/retrieval/profile visibility tests cover canonical wiring; Exchange rule blobs, client-only rules, provider-specific predicates, delegate templates, and deferred actions remain gated on canonical rule/deferred-action semantics. |
| Settings | `server_settings`, mailbox state, `mapi_profile_settings`, computed store/folder defaults | `/api/mail/outlook-profile` read summary and server setting APIs | private read-only `OutlookProfile/*` | Store/logon properties, default-folder properties, IPM subtree OST identity reload | Tests cover profile-state summary and OST identity reuse; full Exchange profile blobs remain gated on canonical profile-state requirements, while client-local registry state is outside server control. |
| Identities | `account_identities`, authenticated account state, sender rights | workspace/session APIs and delegation APIs | `Identity/*` | mailbox owner/user GUID/store identity properties | Covered by identity/delegation tests. |
| Storage/profile state | `mapi_named_properties`, `mapi_custom_property_values`, `mapi_navigation_shortcuts`, `mapi_associated_config_messages`, `mapi_sync_checkpoints`, `mapi_object_identities`, `mapi_special_folder_aliases` | `/api/mail/outlook-profile` read summary | private read-only `OutlookProfile/*` plus object-specific projections | named property mapping, shortcut and associated configuration FAI rows, ICS checkpoints, object IDs/source keys/change keys, and bounded special-folder alias resolution across sessions | Covered by schema/runtime/MAPI profile tests; client-local PST/OST files are intentionally out of scope. |

## Outlook Compatibility Metadata Boundaries

MAPI over HTTP has three separate state boundaries. New Outlook compatibility
work must place data in exactly one boundary before adding persistence or
session handling.

| Boundary | Owns | Tables or storage | Rule |
| --- | --- | --- | --- |
| Canonical mailbox and collaboration state | Mailbox membership, messages, MIME/body/blob rows, submission queue state, contacts, calendars, tasks, notes, journals, search folders, public-folder state, grants, sender rights, and user-visible settings. | Canonical LPE tables such as `messages`, `mailboxes`, `mailbox_messages`, `submission_queue`, `contact_books`, `contacts`, `calendars`, `calendar_events`, `task_lists`, `tasks`, `search_folders`, public-folder tables, grants, `sieve_scripts`, `account_identities`, and `server_settings`. | This is the source of truth. MAPI projects from and mutates this state through canonical APIs or storage paths. No `mapi_*` compatibility table may shadow a canonical field such as subject, body, flags, attendees, permissions, Sent membership, or submission status. |
| Durable Outlook compatibility metadata | Stable protocol identity, bounded profile reuse data, Outlook-only custom properties, associated configuration rows, navigation shortcuts, and resumable EMSMDB/ICS cursors. | `mapi_store_identity`, `mapi_mailbox_replicas`, `mapi_object_identities`, `mapi_special_folder_aliases`, `mapi_named_properties`, `mapi_custom_property_values`, `mapi_profile_settings`, `mapi_folder_profile_property_values`, `mapi_navigation_shortcuts`, `mapi_associated_config_messages`, and `mapi_sync_checkpoints`. | These rows are durable because Outlook cached-mode clients need stable ids, named-property mappings, profile hints, FAI/config replay, alias resolution, and sync cursors across sessions. They do not own mailbox or collaboration content; when a value has a canonical LPE field, the canonical field wins. |
| Session-only MAPI transport state | HTTP request sequencing, cookies, active `MapiContext` and `MapiSequence`, handle tables, open table category/collapse state, pending ROP buffers, transfer-source/collector handles, notification registrations, replay dedupe, and accepted writeback caches that are documented as live-handle-only. | In-memory EMSMDB/NSPI/MAPI session structures and per-handle state only. | Losing this state may force reconnect, replay, or resynchronization, but must not lose committed user data. Session state is persisted only when code explicitly commits a canonical mutation or writes one of the bounded durable Outlook metadata rows above. |

Durable compatibility metadata is intentionally narrower than canonical state:

- `mapi_store_identity` provides the database-wide REPLGUID and GLOBCNT
  allocator; `mapi_mailbox_replicas` binds each account to it; and
  `mapi_object_identities` provides stable FID/MID, SourceKey, ChangeKey, and
  instance-key mappings for canonical or bounded compatibility objects.
- `mapi_special_folder_aliases` stores only immutable, account-scoped alternate
  client FID/SourceKey mappings, plus the server CN required for hierarchy-upload
  state, to reserved canonical special-folder FIDs. It is
  many-to-one toward the canonical folder so independent Outlook profiles and
  OST replicas can coexist. It contains no folder attributes, Calendar or
  Contacts data, membership, rights, FAI payload, or other parallel canonical
  state.
- `mapi_named_properties` stores stable per-account named-property ID
  allocations; active session registries are caches.
- `mapi_custom_property_values` stores opaque Outlook/custom MAPI property
  values only for supported canonical object kinds when no canonical field owns
  that property, plus the explicitly bounded Calendar standard-property
  passthrough set documented above. This includes provider-private named
  metadata on the fixed `LocalFreebusy` Delegate Information object, but never
  its canonical grants, delegate preferences, or durable MAPI identity. Those
  values follow the Message transaction boundary: Set/Delete and
  CopyTo/CopyProperties destination mutations are handle-local, a LocalFreebusy
  copy source reads the effective saved/pending/deleted bag, Release discards
  unsaved changes, and Save atomically publishes them. A
  canonical projection always overrides a stale compatibility value for the
  same property identity.
- `mapi_profile_settings` and `mapi_folder_profile_property_values` store
  bounded profile and folder display metadata needed for cached-mode reopen;
  they must not become arbitrary Exchange profile or folder truth.
- `mapi_navigation_shortcuts` stores Common Views shortcut and group-header FAI
  rows for Outlook navigation-pane compatibility, not canonical folders.
- `mapi_associated_config_messages` stores bounded associated/configuration FAI
  messages for view, form, and client configuration sync replay. These rows are
  not normal mailbox messages and are not exposed through non-MAPI message APIs.
  Their logical key includes folder, message class, and subject because
  MS-OXOCFG view definitions can share `IPM.Microsoft.FolderDesign.NamedView`
  while representing different folder views.
- `mapi_sync_checkpoints` stores operational EMSMDB/ICS completion cursors only.
  They support diagnostics and bounded canonical change-journal work, but they
  never replace the client-uploaded ICS state or select the wire delta. They do
  not store mailbox content.

When a MAPI operation appears to update both Outlook compatibility metadata and
canonical state, the implementation must perform the canonical mutation through
the canonical subsystem first, then update durable compatibility metadata only
for protocol identity, profile reuse, or replay continuity. Metadata-only
client uploads that cannot map to canonical state may be acknowledged only where
this plan explicitly documents that compatibility behavior.

## Outlook Profile Settings Matrix

| Setting area | Canonical storage today | Profile behavior |
| --- | --- | --- |
| Server/bootstrap defaults | `server_settings`, request host/proxy headers, and computed MAPI logon/store properties | Used for URLs, store display metadata, private-store marker, mailbox owner, max submit size, and minimal valid icon payloads. No per-profile copy is stored. |
| Send identities | `account_identities` and authenticated account state | Projected through JMAP/EWS/MAPI identity and submission paths; MAPI does not own a separate identity store. |
| Folder identity and hierarchy | `mailboxes`, built-in projected folder roles, `search_folders`, `mapi_store_identity`, `mapi_object_identities`, and `mapi_special_folder_aliases` | Stable FIDs/source keys/change keys and bounded alternate special-folder aliases are reused across cached-mode sessions. Each default role is converted to its durable mailbox identity at the MAPI boundary; several profile/OST aliases may resolve to one canonical default folder, and the alias table contains no folder content. |
| Custom/shared collaboration folders | `contact_books`, `calendars`, `task_lists`, grants, and `mapi_object_identities` | Non-reserved Outlook-visible collaboration folders use kind-scoped deterministic canonical identity keys and durable store-allocated MAPI object IDs. LPE must not derive folder IDs from raw collection text, owner UUID suffixes, or fallback counters. |
| Named property IDs | `mapi_named_properties` | Durable per-account Outlook named-property ID mapping; session registry is only a cache. |
| Opaque item custom properties | `mapi_custom_property_values` | Stored only for supported canonical item/attachment objects where no canonical field owns the value, the fixed `LocalFreebusy` object's provider-private named metadata, plus the documented bounded Calendar standard-property passthrough set. Canonical grants/preferences and other canonical projections override stale compatibility values. |
| Navigation shortcuts | `mapi_navigation_shortcuts` | Common Views shortcut and group-header FAI rows are durable canonical profile-visible state for cached-mode profile creation and reopen. |
| Folder display flags | `mapi_folder_profile_property_values` | Outlook-written `PidTagExtendedFolderFlags` folder UI streams are persisted per account and MAPI folder id, then overlaid on folder open so display-option writes survive reconnect. This store is bounded to Outlook profile folder flags, not arbitrary Exchange folder truth. |
| Additional Ren Entry IDs | `mapi_folder_profile_property_values`, `mapi_special_folder_aliases`, `mapi_object_identities`, and `mail_change_log` | Inbox `PidTagAdditionalRenEntryIds` always returns canonical values at the five documented positions and preserves opaque later values across abbreviated writes. Root is an input alias for the same Inbox-owned property. Direct writes and hierarchy imports atomically publish the normalized value with its Inbox CN/ChangeKey/PCL/LMT and replay row, and Inbox `folderChange` exports that committed value. A recognized alternate remains a durable redirect and a resident identity for the OST that imported it, but is never projected as a duplicate visible hierarchy row. |
| Associated configuration FAI | `mapi_associated_config_messages` | Outlook-created folder associated/config messages are durable MAPI-only compatibility state for view/form/client configuration sync replay. Direct associated-message deletes are supported and folder-scoped incremental content sync exports associated-config delete idsets. |
| Sync checkpoints | `mapi_sync_checkpoints` | Durable operational EMSMDB/ICS completion cursors for hierarchy/content/read-state diagnostics; they neither store mailbox content nor select a client download delta. |
| IPM subtree OST identity | `mapi_profile_settings.ipm_subtree_ost_id` | Outlook-written cached-mode profile identity is persisted account-wide and reloaded on IPM subtree open after reconnect. |
| Default-folder EntryID writes | computed canonical folder projections plus `mapi_special_folder_aliases` for validated alternate identity | Valid writes are accepted for compatibility. Canonical values stay computed; a validated alternate FID/SourceKey is retained only as a durable account-scoped alias, and invalid values are rejected. |

Normal message contents-table rows project Outlook-selected Inbox view columns
from canonical mail data, including creation time, normal importance,
message size, attachment state, distinct sender and sent-representing fields,
and `PidNameContentClass = urn:content-classes:message`. Per `[MS-OXPROPS]`
sections 2.798, 2.1006, and 2.1018 plus `[MS-OXCMSG]` section 2.2.1.7,
`PidTagMessageSize` is a read-only message property backed by canonical
`messages.size_octets`, `PidTagSender*` uses the canonical `sender` recipient
when present with a `from` fallback, and `PidTagSentRepresenting*` uses the
canonical `from` identity.

## State-Management Invariants

### ICS State Encoding

- Final and checkpoint ICS download state generated by LPE uses REPLGUID-scoped
  IDSET/CNSET encoding for `MetaTagIdsetGiven`, `MetaTagCnsetSeen`,
  `MetaTagCnsetSeenFAI`, and `MetaTagCnsetRead` as applicable to the download
  scope. Final or checkpoint ICS upload state uses only the applicable CN sets
  and never returns `MetaTagIdsetGiven`.
- A successful hierarchy `RopSynchronizationImportDeletes` applies the
  canonical deletion without fabricating a CN from the deleted FID or adding
  that FID to `MetaTagCnsetSeen`. This follows the deletion behavior in
  `[MS-OXCFXICS]` section 3.2.5.9.4.5; that section does not define the deleted
  object identifier as a change number.
- The REPLGUID in durable final/checkpoint state is the LPE replica GUID for the
  relevant mailbox or account scope.
- GLOBSET range commands carry six-byte GLOBCNT values in canonical
  byte-comparison order.
- Transient deleted/read/unread sets use REPLID-scoped IDSET/GLOBSET encoding.
  These transient sets must not be confused with durable REPLGUID checkpoint
  state.
- A hierarchy or contents deletion section serializes `IncrSyncDel (0x40130003)`
  followed by `MetaTagIdsetDeleted (0x67E50102)` and its
  REPLID-scoped IDSET. Property ID `0x4018` belongs to the `FXErrorInfo`
  marker and is never used for `MetaTagIdsetDeleted`. This follows
  `[MS-OXCFXICS]` sections 2.2.1.3.1, 2.2.4.1.4, 2.2.4.2, and 2.2.4.3.3.
- Canonical mail currently has one per-folder modification sequence rather than
  a distinct durable read-state CN. A read/unread transition therefore advances
  the normal message CN and is downloaded as a full `messageChange` carrying
  `PidTagMessageFlags`; LPE must not synthesize a later `IncrSyncRead` from an
  unchanged snapshot. A separate read-state CN remains an optional optimization
  under `[MS-OXCFXICS]` section 3.2.5.6, not parallel mailbox truth.
- Content sync honors Outlook's extra flag contract for `Eid`, message size,
  and change number; when Outlook requests message size in the change header,
  LPE emits a non-zero value for projected normal and associated messages.
- Sync-upload saves that carry only sync identity/state properties and no
  user-visible message data are not persisted as Outlook-visible messages. A
  Deleted Items metadata-only upload that carries an out-of-range source key is
  acknowledged without canonical persistence so Outlook can drain client-local
  cache artifacts without creating user-visible LPE mail; a matching
  out-of-range Deleted Items import-delete is acknowledged as a no-op for the
  same reason. Out-of-range import-delete and read-state cleanup for
  non-persisted associated-message artifacts is also acknowledged as a no-op
  because those identifiers cannot map to canonical LPE state. Deleted Items
  uploads that include user-visible message data are canonical message imports,
  not metadata-only reports.
- Sync-upload saves with `import_associated=true` create or update bounded
  MAPI associated configuration rows when they target a regular folder or a
  supported virtual parent, including Calendar. LPE preserves those rows across
  identity repair and replays them through associated contents and FAI content
  sync, including `MetaTagCnsetSeenFAI`, but keeps them out of canonical mail
  storage and all non-MAPI user-visible surfaces.
- Hierarchy sync emits changed descendant folders of the configured
  synchronization root; it does not emit the synchronization root itself.
  Hierarchy final state scopes `MetaTagIdsetGiven` and `MetaTagCnsetSeen` to
  the emitted descendant folder changes.
- Hierarchy sync loads the complete set of retention-live canonical mailbox and
  search-folder tombstones in scope, without an arbitrary storage page cap, so
  every folder not yet reported as deleted can be downloaded. This follows
  `[MS-OXCFXICS]` section 3.2.5.3.

### Client-State Selection and Checkpoint Advancement

- Download differences are selected only from the initial ICS state uploaded by
  that synchronization context. A zero-length property stream is a valid empty
  set; a server checkpoint is never substituted for it. Malformed
  REPLGUID/GLOBSET state fails the upload-state ROP instead of silently selecting
  a server-side delta. After that `RpcFormat` failure, the configured download
  context remains invalid and neither `RopFastTransferSourceGetBuffer` nor a
  transfer-state handle derived from it can expose the unfiltered manifest; the
  client has to issue a new `RopSynchronizationConfigure`. This follows
  `[MS-OXCFXICS]` sections 3.1.5.4.3.2, 3.1.5.4.3.2.4, and 3.2.5.2.
- For content synchronization, LPE applies `MetaTagIdsetGiven`,
  `MetaTagCnsetSeen`, `MetaTagCnsetSeenFAI`, and `MetaTagCnsetRead`; hierarchy
  applies the first two. Final state is reconstructed from the uploaded sets and
  only the changes and deletions actually downloaded, preserving foreign-replica
  sets. Each downloaded object's `MetaTagIdsetGiven` identity is the exact GID
  carried by its emitted `PidTagSourceKey`, including its foreign REPLGUID when
  `NoForeignIdentifiers` is absent, rather than LPE's internal MID. This follows
  `[MS-OXCFXICS]` sections 2.2.1.1.1, 2.2.1.2.5, 2.2.2.4.2, and 3.2.5.3.
  A durable special-folder alias successfully imported by an OST remains a
  resident identity in that client's hierarchy state even when its canonical
  target is also in the configured projection. Its successful import advances
  `MetaTagCnsetSeen`, and a later download does not report the imported FID in
  `MetaTagIdsetDeleted`. The alias remains a redirect rather than a second
  visible hierarchy row, so another client that never supplied the alias is not
  given it. This keeps the originating OST's default-folder EntryID and
  `MetaTagIdsetGiven` state consistent while canonical folder content remains
  single-owned. This follows `[MS-OXCFXICS]` sections 2.2.3.2.4.3.1, 3.2.5.3,
  3.2.5.9.4.3, and 3.3.5.8.8.
  `MetaTagCnsetRead` remains client-derived unless the transfer contains a
  real separate read-state stream; the current canonical read transition is
  delivered as a full message change with `PidTagMessageFlags`.
  `RopSynchronizationGetTransferState` returns
  the initial state until completion and the same client-derived final state
  afterward. This follows `[MS-OXCFXICS]` sections 2.2.1.1.1 through
  2.2.1.1.4, 3.1.5.2, 3.2.5.2, 3.2.5.3, 3.2.5.6, and 3.2.5.9.3.1.
- `mapi_mailstore/client_state.rs` owns both state selection and the new
  REPLGUID/GLOBSET/FastTransfer wire codec. Before adding further behavior, split
  the codec into `mapi_mailstore/client_state/wire.rs`; keep selection and final
  state reconstruction in `client_state.rs`, with focused tests on each side.
- `mapi_mailstore/diagnostics/codec.rs` owns FastTransfer diagnostic decoding
  and summary validation. Before adding further diagnostics behavior, split the
  state-summary validation into `mapi_mailstore/diagnostics/state.rs` and keep
  the marker/property decoder in `codec.rs`.
- `dispatch/messages.rs` owns Message create/save response and containing-folder
  handle contracts. Before expanding it, move the common create/save response
  and handle helpers into `dispatch/messages/save.rs`; keep Message routing in
  `dispatch/messages.rs`.
- `mapi/session.rs` owns active-session behavior plus handle-slot lifecycle.
  Before expanding it, move handle lookup, response-slot restoration, release,
  and cleanup helpers into `mapi/session/handles.rs`.
- Uploaded client CN sets are parsed as the initial upload checkpoint, not
  copied as opaque bytes or substituted for server-generated state. The final
  upload checkpoint is the semantic set union of each applicable initial CN set
  with the server CNs assigned to successful imports. Uploaded
  `MetaTagIdsetGiven` is ignored.
- `RopSynchronizationGetTransferState` on an ICS upload collector returns
  server-generated checkpoint state. After successful imported message,
  note, journal, read-state, move, delete, or hierarchy changes, the collector
  state is advanced with the server-assigned change numbers in the applicable
  CN sets. No successful or failed upload adds `MetaTagIdsetGiven`; that state
  remains client-owned. Successful delete and source-move uploads still produce
  an explicit server checkpoint, so the transfer-state path does not fall back
  to a stale pre-upload folder snapshot. This follows `[MS-OXCFXICS]` sections
  2.2.3.2.3.1, 2.2.4.4, 3.2.5.2.1, and 3.2.5.9.3.1 and `[MS-OXCROPS]`
  sections 2.2.13.8.1 and 2.2.13.8.2.
- `RopSaveChangesMessage` for an Outlook-uploaded message with an imported
  `PidTagSourceKey`, including uploads into Deleted Items, persists the message
  through canonical mail storage and returns a server-assigned Message ID/change
  number. If the imported source key is a representable LPE replica GID in the
  persisted dynamic range, LPE reserves that identity. If it is system-reserved,
  already allocated, or outside LPE's representable persisted range, LPE assigns
  a new server identity and exposes the source key derived from that identity.
  LPE must not acknowledge a non-metadata ICS upload as saved while keeping it
  only as an unbacked client object, and it must not persist an Outlook-visible
  source key that conflicts with the assigned server Message ID.
- `mapi_sync_checkpoints` stores durable server cursor state: checkpoint kind,
  optional mailbox id, MAPI replica GUID, last canonical change sequence, last
  mail modseq, and a small JSON cursor.
- Hierarchy checkpoints are account-wide and usable only for the same sync root
  and hierarchy cursor version. Content and read-state checkpoints are
  mailbox/folder scoped. Canonical folders use the real mailbox id as the
  durable scope. Virtual special folders, including Calendar, Contacts, Tasks,
  and Reminders, use their stable projected folder UUID as the durable scope.
- On `RopSynchronizationConfigure`, a compatible operational checkpoint can be
  read for diagnostics and completion accounting, but the full current
  canonical scope is compared with the uploaded client ICS sets before any
  FastTransfer bytes are returned.
- A full content snapshot enumerates every visible canonical email in its
  synchronization scope. Table-page limits can bound a table response, but
  must never truncate the source set used to calculate an ICS delta or final
  state.
- The durable checkpoint advances only after `RopFastTransferSourceGetBuffer`
  drains the corresponding ICS download stream.
- Transfer-state handles from download sources retain their checkpoint sequence
  and modseq and must not regress `mapi_sync_checkpoints`.
- Upload/import collector handles mutate canonical mailbox state through the
  import path and must never advance download checkpoints.

### Canonical Change Tracking

- Content sync and folder object lookups select messages from canonical
  per-folder `mailbox_states`. Primary mailbox fields may be used only as a
  compatibility fallback where the canonical membership row is absent.
- Message change numbers include per-folder membership/state facts, not only
  message-body facts.
- Import, save, delete, move, copy, and read-state ROPs mutate canonical mailbox
  state and rely on the same change-log/tombstone path used by other protocols.
- `RopSynchronizationImportReadStateChanges` preflights every resolvable
  message before mutation and returns the exact six-byte response defined by
  `[MS-OXCROPS]` section 2.2.13.3.2. It has no `PartialCompletion` field;
  predictable missing-message failures reject the batch instead of applying a
  successful prefix that cannot be represented on the wire, while requests for
  durable or transient FAI message identities are ignored rather than treated
  as missing normal messages. This follows `[MS-OXCFXICS]` section
  3.2.5.9.4.6.
- MAPI state must remain consistent with JMAP and IMAP-visible state where those
  protocols expose the same user-visible fact.
- `RopModifyRules` is bounded to canonical Sieve-backed mailbox rules. The
  adapter accepts only generated bounded rule definitions that can be translated
  into canonical Sieve text and stored through the existing `sieve_scripts`
  mutation path: move/fileinto, delete/discard, forward/redirect where canonical
  sender rights allow submission, mark-read as a bounded canonical rule action,
  and stop-processing. Exchange-only rule condition/action blobs, client-only
  rules, delegate templates, provider-specific predicates, deferred-action
  provider data, and `RopUpdateDeferredActionMessages` return parseable ROP
  errors and must not create a MAPI-local rule store or activate Sieve.
  `RopGetRulesTable` remains a projection from canonical rule state.
- `RopModifyPermissions` is bounded to Outlook folder ACL rows that identify a
  same-tenant account member through `PidTagMemberId` and supply rights through
  `PidTagMemberRights`. Add and modify rows map read, write, delete, and share
  bits to the canonical `mailbox_delegation_grants` row for the target mailbox
  or the canonical `calendar_grants` row for the target calendar collection;
  remove rows delete that canonical grant. Custom and shared calendar folders
  are accepted only when canonical collection rights include share permission.
  Successful mutations write canonical audit and mail change-log rows and wake
  affected principals through the existing rights journal. Owner, `Default`, and
  `Anonymous` rows are accepted as non-mutating compatibility rows. Unsupported
  member identities, malformed rights, virtual folders, and non-canonical ACL
  data return ROP-specific errors without creating MAPI-local ACL state.

## Release Gates

### Readiness Terms

- Local harness pass means the `crates/lpe-exchange` tests and project live
  scripts pass, including
  `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness`.
- RCA pass means Microsoft Remote Connectivity Analyzer Outlook Connectivity
  passes from the Internet against the same public host and account intended for
  Outlook testing.
- Real Outlook profile pass means Outlook 2016 and Outlook 2019 each create an
  Exchange profile, complete cached-mode synchronization, close and reopen
  without deleting the OST or repairing the profile, resolve NSPI, submit mail
  through canonical LPE submission, and show the authoritative canonical `Sent`
  item.

### Local Harness Gate

- Transport conformance tests cover required headers, `X-RequestId` echoing,
  `X-ClientInfo` echoing, `Content-Length`, `X-ResponseCode`, missing and
  malformed cookies, stale `Disconnect` cookies, duplicate replay, duplicate
  request id with a different body, and overlapping same-session invalid
  sequence behavior.
- EMSMDB tests cover supported bootstrap, hierarchy, contents, table, property,
  FastTransfer, submission, mutation, and unsupported/error paths.
- NSPI tests cover authenticated mailbox and visible-contact resolution,
  reserved hierarchy identifiers, organization-DN non-aliasing, strict
  `GetProps` cardinality/error slots, and deterministic rejection of deferred
  mutation surfaces.
- ICS invariant tests prove REPLGUID final/checkpoint state, REPLID transient
  sets, baseline selection for zero-length client state, delta selection for
  non-empty uploaded state, non-regressing download checkpoints, and no
  checkpoint advancement from upload/import collectors.

### RCA Gate

- The public deployment uses the same host, TLS certificate, account, tenant,
  and endpoint flags intended for Outlook testing.
- Autodiscover publishes only endpoints that are implemented and exposed for the
  gate being tested.
- RCA Outlook Connectivity completes without requiring undocumented local
  registry edits, manual endpoint overrides, or protocol publication that is not
  backed by runtime behavior.

### Outlook 2016/2019 Cached-Mode Evidence

Each supported Outlook version must have separate evidence. A pass for Outlook
2016 does not imply a pass for Outlook 2019, and vice versa.
Use `docs/architecture/outlook-cached-mode-release-evidence-template.md` to record
deployment, autodiscover, local harness, Microsoft RCA, real Outlook profile,
and log evidence. Completing the template does not by itself mark the gate
passed or authorize publication.

Success criteria for each version:

1. A clean Windows profile creates an Exchange account through the documented
   autodiscover path with MAPI over HTTP selected for the mailbox transport.
2. Initial cached-mode synchronization completes for the mailbox root, IPM
   subtree, Inbox, Sent, Drafts, Deleted Items or Trash, Calendar, Contacts,
   Tasks, Notes, Journal, and the supported built-in search/reminder folders.
3. Outlook closes and reopens at least twice without deleting the OST, repairing
   the profile, or forcing a full cache rebuild. The resumed session uses server
   checkpoint/delta behavior and does not duplicate, lose, or resurrect items.
4. NSPI resolves the authenticated mailbox and visible contacts while preserving
   tenant/account visibility boundaries.
5. Sending from Outlook uses canonical LPE submission. The sent item appears in
   authoritative canonical `Sent` and remains consistent when viewed through the
   supported non-MAPI protocols.
6. Cross-protocol changes for read/unread state, flags, moves, copies, deletes,
   drafts, attachments, and protected `Bcc` metadata agree with canonical LPE
   state.
7. Evidence records the Outlook version/build, Windows build, LPE commit/build,
   account and tenant used, public host, endpoint flags, autodiscover response,
   RCA result, local harness result, and relevant server/client logs.

Record new real-client evidence in the release evidence template. Keep raw
traces and logs outside the normative documentation tree; Git history remains
the source for retired investigation narratives.

Calendar troubleshooting diagnostics log the Calendar default folder contract,
projected canonical calendar counts, and hierarchy-sync `PidTagParentSourceKey`
role for each folder row. For a strict hierarchy sync stream that does not
include the configured sync root as a `folderChange`, a folder directly below
that sync root is represented by a zero-length `PidTagParentSourceKey` as
defined by Microsoft ICS. LPE's Outlook cached-mode bootstrap stream
intentionally emits the IPM subtree root row before its children; in that
emitted-root stream, direct children such as Inbox, Calendar, Contacts, and Sync
Issues use the emitted IPM subtree row's `PidTagSourceKey` as
`PidTagParentSourceKey` so Outlook can resolve the hierarchy from the row it just
received. Receive-folder table rows must
keep the fixed FolderId, MessageClass, and LastModificationTime property-row
wire shape, advertise `IPM.Appointment` before the generic `IPM` row, encode
MessageClass as String8, and derive LastModificationTime from canonical folder
change state, so Outlook can resolve the `IPM.Appointment` receive folder to
the advertised Calendar folder. `RopGetReceiveFolder`, `RopSetReceiveFolder`,
and `RopGetReceiveFolderTable` are valid only on the private mailbox logon
handle and return `ecNotSupported` for other handles. RCA diagnostics log the
receive-folder table row count, first message class, Calendar row presence, and
MessageClass wire type so Outlook startup traces can distinguish a missing
Calendar mapping from stale client cache behavior.
Root and IPM subtree `PidTagSubfolders` projections must remain true even in an
otherwise empty canonical mailbox because LPE's virtual Outlook special-folder
tree is always present below those folders; Outlook startup must be able to walk
that tree before any canonical mail rows or calendar events exist. Root and IPM
subtree rows must also project decodeable `PidTagEntryId` and
`PidTagInstanceKey` values so any cached hierarchy identity Outlook captures
during that walk can be reopened later. IPM subtree FastTransfer hierarchy sync
must emit the IPM subtree root row before child folders, with the same generic
folder type used by table/property projections and Root's source key as its
parent source key, so cached-mode Outlook can anchor child default-folder
EntryIDs under a present OST hierarchy parent.
Restricted hierarchy searches over those
rows must match the same display names and identity values that unrestricted
hierarchy `QueryRows` returns, including `"Top of Information Store"` for the
IPM subtree and exact account-scoped `PidTagEntryId` matches for the IPM subtree
and Calendar folder. Inbox hierarchy restriction matching must also evaluate
`PidTagIpmAppointmentEntryId` with the authenticated mailbox GUID for both real
canonical Inbox rows and synthetic virtual Inbox rows. FastTransfer/ICS
hierarchy folder-change rows for virtual parent folders such as Root, IPM
subtree, Sync Issues, and Recoverable Items root must also report
`PidTagSubfolders=true` based on the virtual
special-folder tree, not only on the subset of child rows included in the
current transfer.
`PidTagIpmAppointmentEntryId` projections from
canonical and synthetic Inbox hierarchy `QueryRows`/`FindRow` rows, direct Inbox
property reads, Root fallback reads, and store logon reads must use the
authenticated mailbox GUID consistently so Outlook does not see distinct
Calendar entry IDs for the same default folder. Opened Calendar folder
properties and hierarchy rows must also project decodeable `PidTagEntryId`,
`PidTagInstanceKey`, and `PidTagSourceKey` values for the same canonical folder
object, including `GetPropertiesSpecific`, `GetPropertiesAll`, property-list,
hierarchy-table probes, and ICS hierarchy folder-change rows unless the client
explicitly excludes the property. Calendar folders with `IPF.Appointment`
container class must also project `PidTagDefaultPostMessageClass` as
`IPM.Appointment` in both String8 and Unicode request forms so Outlook binds
the folder's default item type without falling back to generic mail semantics;
the Unicode form must be advertised by folder property enumeration and default
hierarchy column discovery and emitted in hierarchy FastTransfer/ICS folder
changes, not only returned for exact property probes. Hierarchy ICS exclusion
lists are matched through canonical string-property identity, so excluding the
String8 or Unicode form of a folder string property suppresses the same
underlying folder fact instead of reintroducing it through the alternate wire
type. Content ICS property include and exclude lists follow the same canonical
string-property matching, including Calendar `IPM.Appointment` message-class
rows, so String8 client filters and Unicode server projections remain aligned.
Hierarchy table rows must keep Calendar identity and classification on the same
row: `PidTagEntryId`, `PidTagInstanceKey`, `PidTagSourceKey`,
`PidTagContainerClass = IPF.Appointment`, and both String8 and Unicode
`PidTagDefaultPostMessageClass = IPM.Appointment` must describe the same
canonical folder object.
Calendar snapshot identity is request-principal scoped. Custom Calendar FIDs
and Event MIDs/SourceKeys are materialized from that principal's durable SQL
identity records; a process-global canonical UUID alias cannot resolve an
Event, populate a direct property read, feed ICS, or supply a notification
fallback. Event lookup matches the MID stored in the snapshot exactly, and a
missing principal-scoped custom-folder or Event identity fails closed. This
implements the server-compatible Folder ID and Message ID rules in
`[MS-OXCFXICS]` section 3.1.5.3 and the persisted-or-generated
`PidTagSourceKey` rule in `[MS-OXCFXICS]` section 3.2.5.5. The default Calendar
continues to use its reserved `CALENDAR_FOLDER_ID`; only custom Calendar FIDs
require a dynamic principal-scoped identity record.
`RopGetReceiveFolder(IPM.Appointment)` must likewise resolve to the canonical
default Calendar FID and that FID must be immediately openable as
`IPF.Appointment`, even when canonical storage currently contains only custom
calendar collections.
`PidTagIpmAppointmentEntryId` must return an account-scoped EntryID whose
embedded long-term ID converts back to the same canonical Calendar FID; reopening
that FID must expose the same `IPF.Appointment`/`IPM.Appointment` folder
classification.
RCA diagnostics for Outlook Calendar startup must expose the exact folder
discovery path: whether `PidTagIpmAppointmentEntryId` was requested from Inbox
or Root, whether the Root fallback EntryID bytes match the Inbox EntryID bytes,
whether `RopGetReceiveFolder(IPM.Appointment)` resolves to the Calendar FID, and
whether the exact hierarchy-table property set requested by Outlook included
decodeable Calendar `PidTagEntryId`, `PidTagSourceKey`, and `PidTagFolderId`
values.
`RopGetPropertiesSpecific` on Root must return the same canonical
`PidTagIpmAppointmentEntryId` binary value as Inbox; advertising the property in
Root `GetPropertiesAll` or `GetPropertiesList` is insufficient if the specific
property read later returns a flagged missing value.
Outlook client Event Viewer entries such as `WebRequestSemaphore_Open_Error`
and `FindExtensionForRequestFailed` are not server-visible protocol failures by
themselves. Server-side RCA must correlate them against LPE Autodiscover and
MAPI HTTP logs by `x-requestid`, `client-request-id`, `x-trace-id`, user agent,
and MAPI publication fields. Autodiscover logs must show whether MapiHttp was
requested, gate-enabled, and selected, plus the published EMSMDB/NSPI URLs; MAPI
transport logs must show the matching client correlation headers for EMSMDB and
NSPI requests.
Outlook can issue FAI-only content sync (`syncFlags` normal content not
requested) against Deleted Items. When canonical normal messages exist in
Deleted Items, LPE completes the sync source but must not advance the normal
content checkpoint because the client-requested scope suppressed those messages.
Checkpoint storage diagnostics therefore report
`checkpoint_store_status=not_stored_partial_scope`, while the completed sync
summary reports `status=ok_partial_scope_no_checkpoint` together with
`all_sync_sources_completed=true` and an expected partial-scope
not-stored count. This is not a Calendar-folder lookup failure by itself.
Outlook can also upload transient collector artifacts into Deleted Items using
client-local source keys outside LPE's persisted MAPI identity range. LPE
acknowledges those saves with transient object identities but must not import
them into the canonical mailbox or they will surface as user-visible trash
messages.
Outlook 16.0.20131 uploads FAI deletions with
`RopSynchronizationImportDeletes` (`RopId 0x74`) as one required
`PtypMultipleBinary` (`0x00001102`) `TaggedPropertyValue` containing serialized
22-byte GIDs, not as an array of fixed 8-byte MAPI IDs. The ROP reader must
consume the complete tagged value before parsing the next ROP; otherwise GID
bytes can be misread as ROP headers and inflate the response handle table. The
content collector resolves each raw SourceKey against canonical associated
configuration messages before interpreting its global counter, so Outlook-
allocated keys outside LPE's persisted identity range can delete their matching
FAI while already-absent keys remain idempotent. This follows [MS-OXCROPS]
sections 2.2.13.5.1 and 2.2.13.5.2, [MS-OXCFXICS] section
2.2.3.2.4.5.1, and [MS-OXCDATA] sections 2.2.1.3, 2.11.1, and 2.11.4.
Persisted FAI synchronization identity must remain stable across ICS upload and
download. In the 2026-07-14 14:50 Outlook trace, PostgreSQL retained the three
Calendar configuration SourceKeys ending in `73610`, `73611`, and `73612`, but
the content-sync FastTransfer manifest regenerated SourceKeys ending in
`000035`, `000036`, and `000037` from LPE's internal object IDs. Outlook's OST
integrity report consequently identified three Hidden Messages present only on
the server and three present only in the OST. FAI FastTransfer now emits the
persisted `PidTagSourceKey`, `PidTagChangeKey`, and
`PidTagPredecessorChangeList` byte-for-byte and generates local values only when
they are absent. This follows [MS-OXCFXICS] sections 3.1.5.3 and 3.2.5.5.
Outlook Calendar startup can create Freebusy Data view/configuration messages
under the special Freebusy Data folder. Until LPE stores first-class writable
Freebusy Data FAI state, those creates are acknowledged as transient associated
messages and must not be routed into canonical mail or calendar storage. The
same transient handle remains readable for immediate Outlook
`RopGetPropertiesSpecific` calls after `RopSaveChangesMessage`.
Custom and shared calendar hierarchy rows must use owner-scoped decodeable
folder `PidTagEntryId` values rather than
nil-mailbox placeholders so Outlook can reopen the advertised folder identity,
including ICS hierarchy-sync folder-change rows. Custom and shared calendar
folders must also be present in IPM subtree hierarchy sync as `IPF.Appointment`
folder-change rows, not only in hierarchy tables.
Calendar content sync must load canonical calendar events for the Calendar folder
and emit them as normal
`IPM.Appointment` message changes with appointment timing/location properties,
`PidLidAppointmentStartWhole`, `PidLidAppointmentEndWhole`, all-day, busy status,
state flags, and stable `PidLidGlobalObjectId` / `PidLidCleanGlobalObjectId`
values when canonical events exist; a fresh Calendar folder with no events can
return state-only content sync, while a non-empty Calendar folder must not fall
back to generic-message-only projection. Low-LID Calendar named properties such
as `PidLidGlobalObjectId` must be exposed through
`RopGetPropertyIdsFromNames` with assigned named-property IDs in the
named-property range; the LID itself is the property name, not the wire property
ID. Each mailbox's durable GUID/LID-or-name mapping is authoritative: an
existing mapping must be returned unchanged, its inverse lookup must resolve to
the same named property, and its property ID must not be reused for another
named property. Calendar view/configuration and table projections translate
those registered IDs to their internal property definitions without replacing
the mailbox mapping or assuming that a numeric LID is its wire property ID.
This follows [MS-OXCPRPT] sections 2.2.12,
2.2.12.1, 2.2.12.2, 3.1.4.1, 3.2.5.9, and 3.2.5.10, with the PropertyName
structure from [MS-OXCDATA] section 2.6. Outlook's MAPI Calendar property model
also requires appointment start time
to be strictly earlier than end time, so zero-duration canonical events are
projected to MAPI with a minimum one-minute appointment window while leaving the
canonical event unchanged. Bounded MAPI calendar writes update only existing
canonical `calendar_events` columns: subject/normalized subject with an empty
subject prefix, body, HTML body,
start/end through `PidTagStartDate`/`PidTagEndDate` and
`PidLidAppointmentStartWhole`/`PidLidAppointmentEndWhole` plus
`PidLidCommonStart`/`PidLidCommonEnd`, location, all-day,
busy-status-derived canonical status, where `olTentative` maps to `tentative`
and the availability values `olFree`, `olBusy`, `olOutOfOffice`, and
`olWorkingElsewhere` map to `confirmed`, organizer,
required attendees from display/To attendee properties, and optional attendees
from `PidTagDisplayCc` and `PidLidCcAttendeesString`, plus the bounded
`PidLidTimeZoneDescription` string into canonical `time_zone`. Bounded
start/end `PtypTime` values are UTC instants on the wire and are converted to
and from canonical civil time using the supported `UTC` or recurring
`W. Europe Standard Time` rule, including its standard/daylight bias. This
follows `[MS-OXCDATA]` section 2.11.1 and `[MS-OXOCAL]` sections 2.2.1.5,
2.2.1.6, 2.2.1.9, 3.1.5.5, and 3.1.5.5.1. Bounded
`PidLidAppointmentStateFlags` writes map only the meeting/cancel bits; only the
`asfCanceled` bit updates canonical event status to `cancelled`, while unsupported
state bits are rejected without side effects. Calendar reads project those
canonical body, organizer, attendee, and timezone fields through
direct properties, requested contents columns, and FastTransfer/ICS message
properties, including common start/end aliases, the bounded
`PidLidAllAttendeesString`, `PidLidToAttendeesString`, and
`PidLidCcAttendeesString` plus `PidTagDisplayCc` projections from canonical attendee metadata and
timezone description/definition projections from canonical event timezone
state. Saved Calendar items project `PidTagMessageDeliveryTime` from the
durable server creation time rather than the appointment start, following
`[MS-OXOMSG]` section 2.2.3.9. Start/end-display `TZDEFINITION` values use
`0x0641` as the first `TZRULE.wYear`, following `[MS-OXCICAL]` section
2.1.3.1.1.19 and matching the Outlook appointment upload shape. Calendar item
rows project `PidLidSideEffects` with the documented
open-on-delete, copy, move, and context-menu bits from `[MS-OXCMSG]` section
2.2.1.16 so Outlook can attach normal item actions to Calendar contents rows.
Calendar content sync also projects canonical attachment presence from
`calendar_event_attachments` through `PidTagHasAttachments`; attachment table,
open, and read-stream paths use canonical calendar attachment rows. Attachment
mutations are staged on the owning Event or pending-Event handle:
`RopCreateAttachment (0x23)`, `RopSaveChangesAttachment (0x25)`, and
`RopDeleteAttachment (0x24)` update only that handle's attachment overlay.
`RopSaveChangesAttachment` does not persist the child independently; the
parent `RopSaveChangesMessage (0x0C)` atomically commits the attachment
upserts/deletions with the canonical Event transaction, and `RopRelease`
abandons them. The response-handle slot must contain the exact owning Message
object before validation or mutation; on success it remains bound to that
parent Message as required by `RopSaveChangesAttachment`, so another live
Folder or Message cannot authorize or receive the staged child. This follows
[MS-OXCMSG] sections 2.2.3.13 through 2.2.3.15 and
3.2.5.13 through 3.2.5.15, and [MS-OXCROPS] sections 2.2.6.13 through
2.2.6.15.
Bounded `PidLidTimeZoneDescription` and start/end-display `TZDEFINITION`
payloads map to canonical IANA timezone state, and reads regenerate the Outlook
timezone projections. A client write containing `PidLidTimeZoneStruct` remains
rejected, even when a timezone description accompanies it, until that binary
structure can be parsed and mapped without opaque or parallel state.
`PidLidAppointmentRecur` has a parser-backed bounded read/write mapping for
Gregorian daily, weekly, monthly-by-day including month-end, monthly-nth,
yearly-by-month-day, and yearly-nth recurrence patterns, including supported
yearly `BYMONTH` values, into canonical `recurrence_rule`, `recurrence_json`,
deleted-instance `recurrence_exceptions_json` fields, and modified-instance
exceptions that change the occurrence start/end time, subject, or location.
Direct property reads, contents rows that request the property, and
FastTransfer/ICS calendar sync can project the bounded recurrence blob back
from canonical event state. Appointment-like `IPM.Schedule.Meeting.Request`
payloads that contain only the bounded event property subset are canonicalized
as `calendar_events`; bounded meeting responses update canonical attendee
participation status on the existing event; and the bounded import cancellation
path deletes the existing canonical event. Cancellation submitted as a mutation
on an already-open Event handle remains fail-closed at parent Save until
deletion participates in the same staged atomic commit. Modified exceptions
that override body,
reminder, busy status, attachment, or other per-instance fields, Hijri
recurrence, malformed recurrence blobs, unsupported meeting
response/cancel properties, and other unsupported meeting-response or
cancellation payloads remain
unsupported and are rejected with deterministic parseable errors instead of
being stored as opaque MAPI blobs.

`mapi/properties/calendar.rs` has reached the thousand-line split threshold.
This patch moves Calendar read projection into `calendar/projection.rs`; before
adding further Calendar write behavior, move write/import mapping into a focused
`calendar/import.rs` helper and leave `calendar.rs` as shared dispatch and
wiring. Preserve the current canonical Event boundaries and verify the split
with the focused Calendar property/integration tests followed by
`cargo test -p lpe-exchange`.

The real Outlook 15:25 capture on 2026-07-14 isolated appointment creation
before ICS: `RopCreateMessage` returned a pending Calendar Message object, but
`RopOpenStream`/`RopWriteStream` for binary `PidTagHtml` returned
`0x8004010F`; the following `RopSaveChangesMessage` attempts returned
`0x80040102`, and PostgreSQL contained no `calendar_events` row. The live dump
showed Outlook displaying the resulting modal error rather than a memory
crash. Pending Calendar Message objects now support the same writable HTML
stream contract as other Message objects, and saving maps that stream to
canonical `body_html`. Appointment creation accepts the additional named
properties permitted on Message objects, maps supported fields to canonical
event columns, and retains the supplied `PidLidGlobalObjectId` in canonical
`calendar_events.uid` so reads reproduce the identical BLOB without a MAPI-only
event store. This behavior follows [MS-OXCPRPT] sections 2.2.14, 2.2.18,
3.1.4.6, 3.2.5.13, and 3.2.5.15,
[MS-OXCROPS] sections 2.2.9.1, 2.2.9.3, 2.2.9.5, 2.2.6.3, and 2.2.8.6,
[MS-OXCMSG] section 2.2, and [MS-OXOCAL] sections 2.2.1.27, 2.2.1.28, and
4.2.2.1. `PidTagBody`, `PidTagBodyHtml`, and `PidTagHtml` are defined in
[MS-OXPROPS] sections 2.609, 2.621, and 2.734.
An existing Event opened read/write now uses the same Body/HTML stream
surface. `RopCommitStream` fixes the property value in that Event handle's
transaction; database persistence still waits for the parent
`RopSaveChangesMessage`. The staged stream is visible on the same handle,
remains invisible to another handle and PostgreSQL, is discarded by
`RopRelease`, and reaches canonical `body_text` or `body_html` only through
that parent Save.
The same 15:25 request showed that Outlook used the mailbox mappings returned
by `RopGetPropertyIdsFromNames`, rather than the numeric LIDs, when it wrote the
appointment. The observed mappings included `0x90E5 -> PSETID_Appointment
0x820D` (`PidLidAppointmentStartWhole`), `0x90E6 -> 0x820E`
(`PidLidAppointmentEndWhole`), `0x90E8 -> 0x825E` and `0x90E9 -> 0x825F`
(the start/end display `TZDEFINITION` properties), `0x9109 -> 0x8234`
(`PidLidTimeZoneDescription`), `0x900E -> PSETID_Common 0x8503`
(`PidLidReminderSet`), and `0x9132 -> 0x8560`
(`PidLidReminderSignalTime`). Set/delete operations, direct property reads, and
writable Calendar property streams now resolve the mailbox-assigned ID through
the session registry before applying canonical semantics. Unknown named
properties keep their assigned ID and remain bounded custom properties; the
mailbox mapping itself is never rewritten. This follows [MS-OXCPRPT] sections
3.1.4.1 and 3.2.5.10.

For single-instance appointments, the human-readable
`PidLidTimeZoneDescription` captured from Outlook was
`(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna`, while the
accompanying `PidLidAppointmentTimeZoneDefinitionStartDisplay` carried the
stable key `W. Europe Standard Time`. LPE parses only the bounded persisted
`TZDEFINITION` header/key-name shape, maps that Windows key to the CLDR world
IANA mapping `Europe/Berlin`, and stores the IANA key in canonical
`calendar_events.time_zone`; reads map it back to `W. Europe Standard Time`
and regenerate the timezone structures. The exact wire properties and formats
are documented by [MS-OXOCAL] sections 2.2.1.40 through 2.2.1.43. Initial appointment save also
maps `PidLidReminderSet` and `PidLidReminderSignalTime` to the canonical
`calendar_events.reminder_set` and `calendar_events.reminder_at` fields, and
direct GetProps plus ICS read those same fields, following [MS-OXORMDR]
sections 2.2.1.1 and 2.2.1.2.
There is no MAPI-only reminder state.

The named-property normalization is shared by Contacts. Outlook writes for
`PSETID_Address` LIDs `0x8083`, `0x8093`, and `0x80A3` now map Email1, Email2,
and Email3 into canonical `contacts.emails_json`, preserving Unicode names and
multiple addresses as required by [MS-OXOCNTC] section 2.2.1.2. Contact
categories are not claimed by this correction: the current canonical Contact
model has no category field, so adding categories requires a separate
schema/API decision backed by a real Contacts trace rather than a MAPI-only
shadow property.

The 17:14 follow-up trace proved why the Windows/IANA boundary is required.
Build `89e37cd38b98` accepted `PidTagHtml`, both stream writes, and both
`RopSetProperties` calls, but request `:309` returned `0x8004010F` from
`RopSaveChangesMessage`. The pending event carried `W. Europe Standard Time`
into PostgreSQL `AT TIME ZONE`, which accepts the canonical IANA identifier,
and no `calendar_events` row was created. Dump `OUTLOOK (21).DMP` showed the
main thread in `ItemUIHost::HrSaveAndCommitEx` and the modal alert pump, not a
client crash. A PostgreSQL-backed MAPI regression now requires a dynamically
assigned `PidLidAppointmentTimeZoneDefinitionStartDisplay` to save as
`Europe/Berlin` and project back to the Outlook Windows key.

The `202607141822` follow-up isolated the next failure after canonical
appointment persistence. Request `:301` at 18:19:09 executed
`RopWriteStream (0x2D)`, `RopRelease (0x01)`, two
`RopSetProperties (0x0A)`, `RopSaveChangesMessage (0x0C)`, and
`RopGetPropertiesSpecific (0x07)` in order. The save succeeded with flags
`0x0A`, retained message handle `0xBF`, and returned MID
`0x0000000000590001`; the following read of `PidTagChangeKey
(0x65E20102)` on that same handle returned `0x8004010F`. PostgreSQL already
contained the canonical event and MAPI identity, including ChangeKey
`741f6fd38e1a654f9d422dfb451c8f10000000000059`. Dump
`OUTLOOK (22).DMP` retained the same normalized `MAPI_E_NOT_FOUND` in
Outlook's `IMessage::SaveChanges` path while displaying the modal error; it
did not contain a native crash.

ROPs in one buffer are processed sequentially ([MS-OXCROPS] section 1.3),
`KeepOpenReadWrite` keeps the saved Message object open and returns its MID
([MS-OXCMSG] sections 2.2.3.3.1, 2.2.3.3.2, 4.8.1, and 4.8.2), and an ICS
client can immediately retrieve the new `PidTagChangeKey` after that save
([MS-OXCFXICS] sections 2.2.1.2.7 and 3.3.5.11; response shape in
[MS-OXCROPS] section 2.2.8.3.2). LPE therefore overlays the exact event
returned by the store into the request-local MAPI snapshot only after all
canonical save stages succeed. This makes the immediately following
`RopGetPropertiesSpecific` observe the committed object without adding
persistent or parallel Calendar state; the overlay is discarded with the
request.

The `202607142154` follow-up reproduced a distinct, state-dependent save
failure after the preceding same-buffer ChangeKey correction was deployed.
Request `:323` retained pending-event handle `0xC9` and Calendar folder handle
`0x1F`; both `RopSetProperties` calls succeeded, but
`RopSaveChangesMessage (0x0C)` with flags `0x0A` returned `0x8004010F` before
any second canonical event or MAPI identity was committed. Neither this
request nor the successful `202607141822` create supplied
`PidLidGlobalObjectId` or `PidLidCleanGlobalObjectId`. The first create had
therefore persisted the mapping helper's all-zero UUID string as its UID, and
the second create reused that non-empty value until PostgreSQL rejected the
`(tenant_id, owner_account_id, calendar_id, uid)` uniqueness violation.

An absent incoming GOID now remains an empty mapping input so canonical event
creation assigns the new event UUID as its unique UID; a supplied GOID still
takes precedence through the existing mapping path. This is the canonical
per-event fallback, not MAPI-only identity state. It follows the uniqueness and
immutability rules
for `PidLidGlobalObjectId` and the clean-series identity semantics in
[MS-OXOCAL] sections 2.2.1.27 and 2.2.1.28. The failing save/keep-open response
was interpreted according to [MS-OXCMSG] sections 2.2.3.3.1 and 2.2.3.3.2 and
[MS-OXCROPS] sections 2.2.6.3.1 and 2.2.6.3.2. A PostgreSQL-backed MAPI
regression preserves the already-created all-zero-UID event, creates a second
appointment without either GOID property, and requires a successful save plus
a distinct nonzero canonical UID equal to the second event UUID.

The `202607151015` follow-up isolated the first update of an appointment that
had already been saved while Outlook retained its writable handle.
`Test 10:11` was committed canonically as
event `784c0643-32c9-4302-ac32-779505ff404f`, MAPI MID `0x0000000000670001`,
and ChangeKey/SourceKey
`741f6fd38e1a654f9d422dfb451c8f10000000000067`. Outlook retained handle
`0xD9` after the successful first `RopSaveChangesMessage (0x0C)` with
`SaveFlags=0x0A`; no `RopRelease` targeted that handle. Request `:355` then
sent `RopSetProperties (0x0A)` for ten coherent appointment/common/start/end
and reminder times, `RopDeletePropertiesNoReplicate (0x7A)` for the absent
`PidTagReplyRecipientEntries (0x004F0102)`, and another
`RopSaveChangesMessage`. LPE returned `0x80040102`, `0x80040102`, and
`0x8004010F` respectively. PostgreSQL contained the initial event but no
update journal entry or property change. `OUTLOOK (24).DMP` showed
`ItemUIHost::HrSaveAndCommitEx` opening the modal error from the normalized
`0x8004010F`; it was not an Outlook crash or deadlock.

The ten normalized property tags were `PidLidAppointmentStartWhole`
`0x820D0040` and `PidLidAppointmentEndWhole` `0x820E0040`
([MS-OXOCAL] sections 2.2.1.5 and 2.2.1.6), `PidLidClipStart`
`0x82350040` and `PidLidClipEnd` `0x82360040` (sections 2.2.1.14 and
2.2.1.15), `PidLidReminderTime` `0x85020040` and
`PidLidReminderSignalTime` `0x85600040` ([MS-OXORMDR] sections 2.2.1.4
and 2.2.1.2), `PidLidCommonStart` `0x85160040` and `PidLidCommonEnd`
`0x85170040` ([MS-OXOCAL] sections 2.2.1.32 and 2.2.1.33), and
`PidTagStartDate` `0x00600040` and `PidTagEndDate` `0x00610040`
([MS-OXOCAL] sections 2.2.1.30 and 2.2.1.31). The duplicate start/end
values were coherent. The optional Clip values remain bounded named-property
values attached to the canonical Event identity; they do not create a second
calendar item or a session-local appointment model. The same canonical
property set retains the initial `PidLidReminderDelta` `0x85010003`
([MS-OXORMDR] section 2.2.1.3), while the reminder signal time is also mapped
to canonical reminder state.

The failure came from obsolete Calendar-wide core-event mutation guards, not
from a stale handle, access rights, or an event identity mismatch. Each opened
writable Event now owns a `MapiEventTransaction` based on the canonical Event
`modseq`. Property sets, property deletions, and stream writes change only that
handle's staged state; another handle and PostgreSQL continue to expose the
committed Event until `RopSaveChangesMessage`, and `RopRelease` abandons the
staged state. Folder `may_write`/`may_delete` checks remain authoritative, and
a read-only shared-calendar regression covers property update and item
deletion. Named properties which do not map to a modeled Event field are
written to the bounded canonical custom-property store only by the parent
Save, so they survive a restart without creating session-local Calendar state.
Pending Events use the same staged validation: a mixed `RopSetProperties`
retains each valid property and returns a `PropertyProblem` for each invalid
property at its request index. The response restores the exact raw client tag,
including a mailbox-assigned named-property ID, after internal normalization;
this implements [MS-OXCPRPT] section 3.2.5.4 and the `Index`/`PropertyTag`
fields in [MS-OXCDATA] section 2.7.

Deletion follows the representable canonical post-state. Deleting an already
absent projected property is accepted when the canonical Event confirms that
it has no value. Deleting `PidTagSubject (0x0037001F)` stages an empty canonical
title instead of returning a problem, matching [MS-OXCMSG] section 2.2.1.46
and the zero-length `SUMMARY` case in [MS-OXCICAL] section 2.1.3.1.1.20.24.
For the subject pair, `PidTagSubject (0x0037001F)` takes deterministic
precedence over `PidTagNormalizedSubject (0x0E1D001F)` when both are supplied,
independently of their wire order; because the bounded appointment model has
  an empty `PidTagSubjectPrefix`, the stored pair is kept equal. `PidTagDisplayName`
  is not treated as a Calendar subject alias. This follows the relationship in
  [MS-OXCMSG] sections 2.2.1.9, 2.2.1.10, and 2.2.1.46 and [MS-OXOMSG] section
  2.2.1.60. Calendar location uses
only `PidLidLocation (0x8208001F)` from [MS-OXOCAL] section 2.2.1.4; the
unrelated address-book `PidTagLocation` and `PidTagLastModifierEntryId` IDs are
not Calendar aliases. The two documented HTML representations,
`PidTagBodyHtml (0x1013001F)` and `PidTagHtml (0x10130102)`, are maintained as a
coherent staged pair under [MS-OXCMSG] sections 2.2.1.58.3, 2.2.1.58.9, and
3.2.4.1, so deleting one representation and setting the other cannot resurrect
stale handle state. Deleting
`PidLidReminderSet (0x8503000B)` is
represented canonically as `FALSE`; independent deletion of
`PidLidReminderDelta (0x85010003)`, `PidLidReminderTime (0x85020040)`, or
`PidLidReminderSignalTime (0x85600040)` reports a `PropertyProblem` while a
reminder is active, but is idempotent after the reminder is disabled or when
it was already absent. These reminder rules use [MS-OXORMDR] sections 2.2.1.1
through 2.2.1.4 and 3.1.4.1.3. Other present properties that have no
representable deletion continue to report the underlying error. The Microsoft
documents require a property to have no value after successful deletion but do
not explicitly make repeated deletion idempotent, so absent-property
acceptance remains an Outlook interoperability inference from [MS-OXCPRPT]
section 3.2.5.5 rather than a quoted normative requirement.

Saving an initial pending Event now creates the canonical Event, its first MAPI
identity and durable version, reminder state, bounded custom properties, and
canonical attachments plus the change-log row in one PostgreSQL transaction.
A failed transaction leaves none of those artifacts, so retry creates one
Event rather than exposing a partial or duplicate appointment. For shared
calendars, the Event, reminder, custom properties, and attachments remain
owned by the canonical calendar owner while the client principal receives its
scoped MAPI identity.

Saving an existing active or deleted Event now calls one PostgreSQL transaction
that locks the canonical row, checks the handle's expected `modseq`, applies
the staged core,
reminder, custom-property, and attachment changes, advances the Calendar
modseq, writes the canonical change log, and rotates every principal-scoped
Event MAPI identity for that lifecycle. A stale handle returns
`ecObjectModified (0x80040109)` without
overwriting the newer Event or clearing the stale handle's staged state.
`ForceSave (0x04)` bypasses only that object-modified check and commits the
already-staged values as a new version. After a successful
`KeepOpenReadWrite` save, the retained handle is reset to the newly committed
`modseq` and exposes the new version immediately.

Calendar Event version metadata is durable rather than synthesized from the
session. The canonical `calendar_events.modseq` remains the CAS token, while
`mapi_mailbox_replicas` allocates a new 48-bit change number and the current
`mapi_object_identities` rows persist `PidTagChangeNumber`, a new
`PidTagChangeKey`, and the predecessor list merged with that new XID in the same
transaction. Each principal-scoped Event MID and `PidTagSourceKey` remains
stable for an in-place Save; this does not apply to an inter-Calendar move,
which requires a new destination MID. `PidTagLocalCommitTime (0x67090040)` is
projected from the durable version's UTC update time immediately after Save
and after reopen, rather than from the appointment start time, as defined by
[MS-OXPROPS] section 2.774 and [MS-OXCMSG] section 2.2.1.49. Direct property
mutation rejects the server-managed `PidTagSourceKey`, `PidTagChangeKey`,
`PidTagPredecessorChangeList`, `PidTagChangeNumber`,
`PidTagLastModificationTime`, and `PidTagLocalCommitTime` values with exact
`PropertyProblem` indexes and client tags. PostgreSQL keeps the canonical Event
commit time strictly monotonic under the Event row lock and stores the distinct
MAPI version LMT on `mapi_object_identities`: imports retain the accepted LMT,
while direct changes assign server time. Direct property reads and Calendar
ICS/FastTransfer consume this persisted LMT and CK/CN/PCL state, so the version observed after Save is also the version loaded after
restart. Reopening an existing identity preserves its valid persisted material:
SourceKey/InstanceKey remain tied to the immutable object counter, a normal
Calendar import keeps its structurally valid imported ChangeKey/PCL/LMT current
beside the distinct server CN, and a later direct modification advances the CN,
server ChangeKey, and version LMT. `PidTagLastModificationTime` exposes that
identity LMT; `PidTagLocalCommitTime` exposes the separate Event commit time. The fresh `0.5.2-sql`
baseline has no legacy identity-material migration path, so identity lookup
does not infer or silently repair arbitrary manually corrupted CK/CN/PCL
combinations. Canonical mutation and import paths must persist each intended
version transition atomically. This implements
the identity/version contracts in [MS-OXCFXICS]
sections 2.2.1.2.3, 2.2.1.2.5, 2.2.1.2.7, 2.2.1.2.8, 2.2.2.1, 2.2.2.2, 2.2.2.3,
2.2.2.3.1, 2.2.2.5, and 3.1.5.3. Source-key generation follows
[MS-OXCFXICS] section 3.2.5.5 separately.

The `202607162052` real Outlook delete capture isolated a distinct
`RopMoveCopyMessages (0x33)` path. Outlook 16.0.20131 opened Event MID
`0x0000000000460001` with ChangeKey suffix `0x56`, then issued a synchronous
move (`WantAsynchronous=FALSE`, `WantCopy=FALSE`) from Calendar FID
`0x0000000000100001` to Deleted Items FID `0x0000000000080001`. LPE routed the
Event MID through the generic `JmapEmail` lookup, returned `ReturnValue=0` with
`PartialCompletion=TRUE`, and made no PostgreSQL mutation. Outlook correctly
surfaced that partial result as `MAPI_E_EXTENDED_ERROR (0x80040119)` and an
`Unknown Error` dialog.

Calendar-to-Deleted-Items moves now preserve one canonical Event row while
changing its lifecycle from `active` to `deleted`. The inter-folder move
allocates a new principal-scoped MID, SourceKey, ChangeNumber, ChangeKey, and
InstanceKey for the destination object, records the old/new identity lineage,
and retains the old MID for the source-folder ICS deletion. Deleted Items
projects that same Event as `IPM.Appointment` beside canonical mail rows; it
does not create a parallel or synthetic message. The response returns
`PartialCompletion=FALSE`, the source emits a deletion, and the destination
emits a moved/created notification and ICS change with the new identity.
Deleted Items merges mail and Calendar rows in one bounded, globally sorted
table window, including categorized expand/collapse views. A restarted session
reloads the durable destination CN/ChangeKey/PCL and emits Event attachments in
the destination FastTransfer stream. Live notifications are reconstructed from
the canonical change log and the identity-move lineage, so the author session
and other active sessions observe the same old/new FID/MID pair.

This identity transition follows [MS-OXCFXICS] section 3.1.5.3, which requires
a new internal identifier for an inter-folder move, and source-key generation
follows sections 2.2.1.2.5 and 3.2.5.5. The request/response and
partial-completion contracts are [MS-OXCROPS] sections 2.2.4.6.1 and
2.2.4.6.2 and [MS-OXCFOLD] sections 2.2.1.6.1, 2.2.1.6.2, 3.1.4.6, and
3.2.5.6. FID/MID pairs follow [MS-OXCDATA] sections 2.2.1.1 and 2.2.1.2, and
the Deleted Items role follows [MS-OXOSFLD] section 2.2.1. Moved-object
notification identities follow [MS-OXCNOTIF] sections
2.2.1.1, 2.2.1.4.1.2, and 3.1.4.3. Calendar copies, moves to unrelated mailbox
folders, restore from Deleted Items, and final deletion from Deleted Items
remain separate interoperability work. The captured regression is
`mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event`.
The mixed table regression also applies [MS-OXCTABL] sections 2.2.2.3,
2.2.2.5, 2.2.2.17, 2.2.2.19, 2.2.2.20, and 4.5.

Three Calendar mutation paths remain deliberately incomplete:

- Meeting cancellation on an existing Event handle remains fail-closed because
  canonical deletion is not yet part of the staged CAS/version transaction.
- `PidLidTimeZoneStruct` writes remain fail-closed; LPE can project a structure
  from canonical timezone state but does not yet parse and round-trip an
  arbitrary client-supplied structure into that state.
- Moving an Event between Calendar folders does not yet retire the old internal
  Message ID and allocate a new destination Message ID. Until the move,
  notification, and ICS paths share that atomic identity transition, an
  inter-folder move remains outside the validated Outlook gate required by
  [MS-OXCFXICS] section 3.1.5.3 and [MS-OXCNOTIF] section 2.2.1.4.1.2.

Event rows in the Reminders search folder still need matching table, ICS,
delta, and deletion behavior. A successful Event Save queues the live-session
Calendar `TableModified` event and writes the canonical `calendar_event`
change-log row. Notification polling translates durable Event create, update,
and delete rows into `ObjectCreated`, `ObjectModified`, or `ObjectDeleted` data
with stable in-place Message IDs and the Calendar folder ID scoped to the
principal receiving the notification. A Calendar `moved` row remains
fail-closed until the durable transition carries the destination `MessageId`
and source `OldMessageId` fields defined by [MS-OXCNOTIF] section 2.2.1.4.1.2,
with the new inter-folder identity required by [MS-OXCFXICS] section 3.1.5.3.
It is never serialized as `ObjectMoved` by reusing the same MID for both
fields.
Delete tombstones retain the collection, UID,
affected-principal set, and retired Event identity needed to emit the final
notification without recreating the Event. This implements the bounded object
notification data in [MS-OXCNOTIF] sections 2.2.1.1 and 2.2.1.4.1.2.
Registrations remain session-local, so clients still re-register after
reconnect and use ordinary ICS as the durable convergence path.

The wire and object contracts used here are [MS-OXCROPS] sections 2.2.8.6.1
through 2.2.8.6.3 (`RopSetProperties`), 2.2.8.9.1 through 2.2.8.9.3
(`RopDeletePropertiesNoReplicate`), and 2.2.6.3.1 through 2.2.6.3.3
(`RopSaveChangesMessage`); [MS-OXCPRPT] sections 2.2.5, 2.2.7, 2.2.8,
3.2.5.4, and 3.2.5.5; and [MS-OXCDATA] sections 2.4.2 and 2.7 for
`PropertyProblem`. `KeepOpenReadWrite` is bit `0x02` in [MS-OXCMSG] section
2.2.3.3.1, not the complete value `0x0A`. For Outlook's captured `0x0A`, this
correction relies on bit `0x02`. The combined value `0x0A` is labeled
`KeepOpenReadWrite DelayedCall` in [MS-OXOPFFB] section 4.1 and [MS-OXODLGT]
section 4.1.3.2; no separate `0x08` semantic is relied on.
For Outlook's captured Contacts FAI `SaveFlags=0x0E`, the unlisted `0x08` bit
is likewise ignored as required by [MS-OXCMSG] section 2.2.3.3.1; the remaining
`ForceSave` bit `0x04` subsumes the compatible `KeepOpenReadWrite` bit `0x02`
because both retain read/write access. `ForceSave` performs the explicit
stale-version overwrite described above when a version conflict exists. The
retained-handle, conflict, and forced-save server behavior follows [MS-OXCMSG]
section 3.2.5.3 and is illustrated by sections 4.8.1 and 4.8.2.
`PidTagReplyRecipientEntries` is defined by
[MS-OXPROPS] section 2.917 and [MS-OXOMSG] section 2.2.1.43 as property ID
`0x004F`, `PtypBinary (0x0102)`, containing the [MS-OXCDATA] section 2.3.3
`FlatEntryList`. The semantic regressions are
`mapi_over_http_calendar_keep_open_handle_accepts_second_update_save`,
`mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards`,
`mapi_over_http_calendar_concurrent_rw_handles_require_force_save`,
`mapi_over_http_outlook_contact_prefs_save_accepts_combined_force_flags`,
`mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties`,
`mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields`,
and `mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder`.
Attachment regressions cover handle-local create/delete overlays, Release
discard, and atomic initial/existing parent-Event Save.
PostgreSQL regressions cover one atomic version, canonical-writer version
advance, stale-CAS/ForceSave behavior, and rollback when change-number
allocation fails. The captured JSONL files are diagnostic records, not a
self-contained HTTP replay, because they do not preserve the complete
authenticated session and dynamic handle/ChangeKey chain.

`dispatch/properties.rs` remains over the production line target. This patch
moves direct property reads into `dispatch/property_reads.rs`; keep the existing
dispatch entry point as a thin router and move further cohesive read behavior to
that helper. The Event save work is already split into helper modules and this
correction does not add implementation to `mapi.rs`.
`mapi/dispatch.rs` is now below the hard production limit. Execute transport
response orchestration and its post-CommonViews handoff logging live in the
existing `dispatch/execute.rs` helper; `dispatch.rs` retains the module wiring,
shared dispatch context, and top-level `execute_rops` ROP orchestration.
`mapi/dispatch/table_controls.rs` is also below the hard production limit;
status and bookmark lifecycle routing now lives with the existing
`dispatch/table_lifecycle.rs` helper while table mutation/query behavior stays
in `table_controls.rs`.
`mapi/dispatch/folders.rs` has crossed the thousand-line review threshold. Its
next split must move delete, empty, and move/copy mutation handlers into
`dispatch/folders/mutations.rs`, leaving folder lookup and response-routing
helpers in the parent.
`mapi/dispatch/attachments.rs` has crossed the thousand-line review threshold.
The SaveChangesAttachment response-handle contract already lives in
`dispatch/attachments/save_contract.rs`; the next split must move attachment
open/read/table projection into `dispatch/attachments/read.rs`, leaving staged
mutation routing in the parent.
`mapi/rop.rs` has crossed the thousand-line review threshold. Its next split
must move Message, Folder, Attachment, and Logon property serialization into
`mapi/rop/property_serialization.rs`, retaining only shared ROP response wiring
in the parent.
`mapi/store_adapter/access_plan.rs` has crossed the thousand-line review
threshold. Its next split must move request-handle simulation and issued-handle
tracking into `mapi/store_adapter/access_plan/handles.rs`, leaving access-plan
construction and store-load selection in the parent.
`mapi/dispatch/event_transactions.rs` has crossed the thousand-line review
threshold while consolidating Calendar Set/Delete/FastTransfer invariants. Its
next split must move create-time/import normalization and property validation
into `dispatch/event_transactions/import.rs`, leaving saved-event optimistic
commit construction and post-commit projection in the parent.
Calendar attachments are projected only through canonical
`calendar_event_attachments`: `PidTagHasAttachments`,
`RopGetValidAttachments`, `RopGetAttachmentTable`, and `RopOpenAttachment`
read that table. Calendar `RopCreateAttachment`, `RopSaveChangesAttachment`,
and `RopDeleteAttachment` use a per-parent-handle overlay that is visible to
that handle's attachment reads but not to another handle or PostgreSQL before
the parent `RopSaveChangesMessage`. The parent Save commits the overlay through
the same canonical Event transaction; no Outlook-only attachment state is
stored.

Delegate/free-busy readiness additionally requires the canonical
`/api/mail/delegation/free-busy` layer to return delegate access objects and
merged non-overlapping availability blocks for the target mailbox calendar.
When no canonical delegate or free/busy state exists, the data-derived
delegate message list is empty. Freebusy Data nevertheless retains its one
documented Delegate Information projection, `LocalFreebusy`, so the Root/Inbox
`PidTagFreeBusyEntryIds` contract never advertises a dangling EntryID. Its
protocol identity/version is durable and account scoped; its contents are
computed from canonical default-Inbox/default-Calendar grants, account-wide
sender rights, accounts, and delegate preferences. A monotonic canonical
delegation revision invalidates that projection even when the final relation is
deleted or a projected delegate's directory fields change. The current/applied
revision pair atomically fences the canonical tuple read and MAPI version
rotation. LPE does not persist a MAPI-local delegate/free-busy content table, and
ordinary calendar-event changes do not rotate the Delegate Information object.
This follows the Microsoft MAPI over HTTP session model, the delegate calendar
constraints in MS-OXODLGT, the delegate-management contract in MS-OXWSDLGM, and
the Outlook free/busy block behavior described by Microsoft's Free/Busy API
documentation. Public MAPI publication still waits for the existing local, RCA,
and real-Outlook evidence gates.

### Publication Gate

- MAPI/HTTP readiness evidence includes the bounded Gate 1 harness, separate
  Microsoft RCA evidence, and separate Outlook 2016 and Outlook 2019
  cached-mode evidence for the deployment class being advertised. The Gate 1
  harness does not substitute for the other evidence.
- `LPE_AUTOCONFIG_MAPI_ENABLED` controls whether MAPI endpoints are advertised.
- `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` remains the legacy
  `EXPR`/RPC over HTTP evidence flag; it does not control MAPI/HTTP publication.
- RPC/HTTP `EXPR` publication requires separate Outlook Anywhere evidence and
  must not be enabled by the MAPI gate alone.
