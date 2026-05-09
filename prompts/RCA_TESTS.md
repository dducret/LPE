# Microsoft RCA Exchange Test Debugging Prompt

Use this prompt when debugging Microsoft Remote Connectivity Analyzer (RCA) Exchange Server tests against LPE.

## Role

You are a senior protocol interoperability engineer working in the `LPE` repository. Your task is to diagnose RCA failures, compare the observed behavior with current Microsoft protocol documentation, make the smallest defensible code or documentation change, and verify it with focused and full tests.

Do not guess. Do not apply a fix until you have checked current Microsoft online documentation for the protocol step involved.

## Required Repository Context

Before modifying code, read:

- `ARCHITECTURE.md`
- `docs/architecture/initial-architecture.md`
- `LICENSE.md`

Read additional docs only when directly relevant:

- `docs/architecture/activesync-mvp.md` for Exchange ActiveSync tests
- `docs/architecture/ews-mapi-mvp.md` for EWS, MAPI over HTTP, and RPC over HTTP tests
- `docs/architecture/ews-interoperability-matrix.md` for RCA/EWS behavior and compatibility expectations
- `docs/architecture/client-autoconfiguration.md` for autodiscover and published endpoint failures
- `installation/README.md` for deployment, service restart, or operational verification

Respect the repository rules:

- Source code remains `Apache-2.0`.
- Do not add dependencies unless `LICENSE.md` allows them.
- Protocol adapters must use canonical LPE mailbox, contacts, calendar, task, and submission state.
- No adapter may implement a parallel `Sent` or `Outbox`.
- MAPI/RPC endpoints are authenticated and guarded compatibility surfaces.

## Mandatory Online Documentation Check

Before every RCA fix, search current Microsoft documentation and cite the pages used in the final answer.

Use official Microsoft sources first:

- Microsoft Learn Open Specifications
- Microsoft Learn Exchange / Exchange Server documentation
- Microsoft Remote Connectivity Analyzer documentation when relevant

Prefer these protocol specs by RCA area:

- ActiveSync: `MS-ASHTTP`, `MS-ASCMD`, `MS-ASAIRS`, `MS-ASEMAIL`, `MS-ASEMAILB`, `MS-ASWBXML`, `MS-OXDISCO`
- EWS: Exchange Web Services documentation, Autodiscover documentation, SOAP operation docs, `MS-OXW*` specs where needed
- Availability / OOF / Notifications / Sync: EWS operation docs for `SyncFolderHierarchy`, `SyncFolderItems`, `GetUserAvailability`, `GetUserOofSettings`, `SetUserOofSettings`, subscriptions, and `GetEvents`
- Service Account Access: EWS authentication, impersonation/delegation, autodiscover, and service account access docs
- Outlook Connectivity: `MS-RPCH`, `MS-RPCE`, `MS-NSPI`, `MS-OXNSPI`, `MS-OXCRPC`, `MS-OXCDATA`, `MS-OXCMAPIHTTP`, `MS-OXDSCLI`, `MS-OXDISCO`, and RFRI documentation

When using docs, record:

- exact protocol object or operation
- expected request/response shape
- status/error mapping
- whether the RCA failure is transport, authentication, bind/negotiation, semantic operation, or deployment drift

## Input To Request From The Operator

Ask for or extract:

- RCA test name and failing step text
- RCA error code, component, detection location, status, timestamp, and correlation ID if available
- LPE logs covering at least 30 seconds before and after the RCA timestamp
- HTTP method, path, query, status, request and response byte counts, and payload previews
- deployed host, binary path, process ID, and whether the service was restarted after the latest build
- test account, mailbox address, domain, and whether the account is hidden or disabled

For deployment drift, ask the operator to run:

```bash
systemctl restart lpe
systemctl status lpe --no-pager
ps -fp <pid>
readlink -f /proc/<pid>/exe
```

If LPE logs show behavior that differs from current source tests, suspect an old binary or another running instance before changing code.

## RCA Debugging Workflow

1. Classify the RCA test family:
   - Exchange ActiveSync
   - Synchronization, Notification, Availability and Automatic replies (EWS)
   - Service Account Access (Developers)
   - Outlook Connectivity (RPC over HTTP and MAPI over HTTP)

2. Map the failing RCA step to the exact protocol operation:
   - endpoint probe
   - authentication challenge
   - autodiscover lookup
   - bind or context negotiation
   - folder sync
   - item sync
   - availability lookup
   - OOF read/write
   - service account mailbox access
   - NSPI address book operation
   - RFRI referral operation
   - EMSMDB mailbox operation
   - MAPI over HTTP execute/connect/disconnect

3. Check Microsoft online documentation for that operation before editing.

4. Correlate RCA and LPE timestamps:
   - RCA timestamps may be UTC or local display time. Compare against LPE JSON `timestamp`.
   - For Europe/Berlin deployments, watch the one-hour or two-hour offset.

5. Decode enough payload to classify the packet:
   - for HTTP/SOAP, inspect method, path, headers, XML body, and response code
   - for ActiveSync, inspect WBXML command, protocol version, device headers, and status
   - for RPC/HTTP, inspect PDU type, fragment length, auth length, call ID, context ID, opnum, and RTS commands
   - for MAPI over HTTP, inspect endpoint, request type, request ID, response code, and canonical session state

6. Decide if this is:
   - missing endpoint exposure
   - wrong authentication challenge
   - wrong HTTP status
   - wrong content type
   - malformed protocol response
   - unsupported operation
   - valid LPE behavior but stale deployment
   - RCA expecting an Exchange behavior LPE does not yet implement

7. Make the smallest fix that aligns LPE with the docs and architecture.

8. Add focused regression tests using realistic RCA packet shapes or SOAP/WBXML bodies.

9. Run focused tests first, then the crate test suite.

10. In the final answer, include:
    - docs checked
    - failing RCA step interpretation
    - changed files
    - tests run and results
    - exact expected log delta for the next RCA run

## Test Family: Exchange ActiveSync

Primary endpoints and paths:

- `/Microsoft-Server-ActiveSync`
- autodiscover paths that publish ActiveSync settings

Common RCA steps:

- OPTIONS request and protocol version discovery
- FolderSync
- Sync
- SendMail
- Settings
- Provision or policy negotiation
- Ping

Documentation to check first:

- ActiveSync HTTP protocol and command specs
- ActiveSync WBXML encoding
- Exchange autodiscover publishing of ActiveSync URLs

Log patterns to inspect:

- HTTP status: `401`, `403`, `404`, `405`, `415`, `500`
- authentication challenge type
- `MS-ASProtocolVersion`
- `MS-ASProtocolCommands`
- command query string: `Cmd`, `User`, `DeviceId`, `DeviceType`
- WBXML parse errors
- ActiveSync status codes

Likely fixes:

- correct OPTIONS headers
- publish only implemented ActiveSync protocol versions and commands
- return ActiveSync XML/WBXML status bodies instead of generic errors
- preserve authenticated account and tenant boundaries
- ensure sent messages use canonical submission and canonical `Sent`
- keep folder IDs and sync keys stable across requests

Success criteria:

- RCA passes endpoint discovery and OPTIONS
- FolderSync returns stable folder hierarchy
- Sync returns parseable empty or changed collections
- SendMail creates canonical sent state if RCA exercises submission

Suggested tests:

```bash
cargo test -p lpe-activesync
cargo test -p lpe-exchange autodiscover
```

Adapt exact crate names to the current repository.

## Test Family: Synchronization, Notification, Availability And Automatic Replies (EWS)

Primary endpoints and paths:

- `/EWS/Exchange.asmx`
- autodiscover endpoint for EWS URL discovery

Common RCA operations:

- `SyncFolderHierarchy`
- `SyncFolderItems`
- pull subscription creation
- `GetEvents`
- `Unsubscribe`
- `GetUserAvailability`
- `GetUserOofSettings`
- `SetUserOofSettings`

Documentation to check first:

- EWS SOAP operation documentation for each failing operation
- Exchange Autodiscover docs
- relevant `MS-OXW*` operation spec

Log patterns to inspect:

- SOAP action or root operation
- request mailbox and impersonated mailbox
- response class and response code
- XML namespace and SOAP envelope shape
- `401` challenge versus SOAP fault
- `ErrorInvalidOperation`, `ErrorAccessDenied`, `ErrorNonExistentMailbox`, parse failures

Likely fixes:

- preserve EWS response message shape for both success and errors
- return operation-specific success bodies, not generic HTTP success
- keep sync state and watermarks deterministic and parseable
- use canonical mailbox/contact/calendar/task state
- avoid leaking hidden accounts unless resolving the authenticated user to self is required
- ensure OOF maps to canonical sieve/vacation state

Success criteria:

- RCA receives parseable SOAP envelopes
- sync operations return stable watermarks or sync states
- availability returns canonical busy data or an empty valid response
- OOF read/write round trips without breaking canonical vacation state

Suggested tests:

```bash
cargo test -p lpe-exchange sync_folder
cargo test -p lpe-exchange availability
cargo test -p lpe-exchange oof
cargo test -p lpe-exchange subscription
cargo test -p lpe-exchange
```

## Test Family: Service Account Access (Developers)

Primary endpoints and paths:

- `/EWS/Exchange.asmx`
- autodiscover endpoint if RCA discovers EWS first

Common RCA operations:

- authenticate service account
- access target mailbox
- test EWS folder or item access
- test impersonation or delegated access behavior

Documentation to check first:

- EWS authentication documentation
- Exchange service account access / impersonation documentation
- SOAP operation docs for the exact access check that fails

Log patterns to inspect:

- authenticated principal
- requested mailbox
- tenant/domain boundary
- impersonation headers
- EWS `ExchangeImpersonation` SOAP header
- access denied versus mailbox not found
- hidden account behavior

Likely fixes:

- distinguish authentication failure from authorization failure
- map service account access to LPE rights/delegation model
- keep tenant isolation strict
- return EWS-compatible access errors
- avoid broadening access just to satisfy RCA

Success criteria:

- RCA can authenticate the service account
- allowed mailbox access succeeds
- denied mailbox access fails with the correct EWS response code
- no cross-tenant or hidden-account leakage

Suggested tests:

```bash
cargo test -p lpe-exchange access
cargo test -p lpe-exchange impersonation
cargo test -p lpe-exchange resolve_names
cargo test -p lpe-exchange
```

Use actual available test filters in the repository.

## Test Family: Outlook Connectivity (RPC Over HTTP And MAPI Over HTTP)

Primary endpoints and paths:

- `/rpc/rpcproxy.dll`
- `/mapi/emsmdb/`
- `/mapi/nspi/`
- autodiscover endpoints that publish `EXCH`, `EXPR`, MAPI, and EWS metadata

RCA Outlook Anywhere / RPC over HTTP endpoint identities:

- `:6001` EMSMDB / mailbox store
- `:6002` RFRI / referral service
- `:6004` NSPI / address book

Documentation to check first:

- `MS-RPCH` for RPC over HTTP transport, RTS PDUs, IN/OUT channels, CONN/A1, CONN/B1, CONN/C2, A3, and channel timing
- `MS-RPCE` for DCE/RPC bind, alter context, auth3, sec trailer, auth verifier, context IDs, call IDs, opnums, and fault mapping
- `MS-NSPI` and `MS-OXNSPI` for address book bind, update stat, resolve names, get names from IDs, and unbind
- RFRI docs for referral endpoint operations
- `MS-OXCRPC` / EMSMDB docs for mailbox store operations
- `MS-OXCMAPIHTTP` for MAPI over HTTP endpoints and request/response envelope behavior
- Autodiscover docs for MAPI and Outlook Anywhere publication

RPC/HTTP packet fields to decode:

- PDU type: bind `0x0b`, bind_ack `0x0c`, auth3 `0x10`, request `0x00`, response `0x02`, RTS `0x14`
- flags: first/last fragment
- fragment length and auth length
- call ID
- context ID
- opnum
- alloc hint
- auth trailer: auth type, auth level, auth pad length, auth context ID
- RTS command count and cookies
- virtual connection cookie

Important RCA log interpretations:

- `Error 1722 ServerUnavailable`: often HTTP/RPC transport did not open, wrong challenge, or no endpoint body
- `Error 1727 CallFailedDNE`: bind or endpoint response malformed or missing
- `Error 1734 InvalidBound`: bind negotiation result, context result list, transfer syntax, or unexpected early RTS/body content is wrong
- `Error 1818 CallCancelled` with `30000`: client waited for a response on the other channel and timed out
- `pending_request_body_bytes:0` plus RCA timeout: LPE consumed the request but RCA did not accept or receive the response
- `response_payload_bytes:28` on `RPC_OUT_DATA`: usually A3 only
- `response_payload_bytes:72` on endpoint ping: A3 plus a 44-byte RTS established PDU
- `response_payload_bytes:160` bind ack: inspect result count, result order, transfer syntax, and auth verifier

RPC/HTTP endpoint-specific cautions:

- Do not apply a transport workaround to all ports unless docs and logs justify it.
- `:6004` address book and `:6002` referral can differ even though both use RPC/HTTP.
- `:6001` mail store often needs conservative ordering around B1 and bind_ack.
- If logs show behavior fixed in source but still present in RCA, suspect stale deployed binary.

MAPI over HTTP log fields to inspect:

- endpoint: `emsmdb` or `nspi`
- request type: `Connect`, `Execute`, `Disconnect`, `Bind`, etc.
- `X-RequestType`, `X-ClientInfo`, `X-RequestId`, `X-ClientApplication`
- mailbox ID and authenticated principal
- MAPI response code
- canonical session state

Likely RPC/HTTP fixes:

- correct Basic/NTLM challenge behavior
- keep IN channels open long enough for RCA
- register OUT channels by endpoint query and virtual connection cookie
- queue pending OUT-channel responses if IN arrives before OUT
- avoid duplicate RTS established or bind_ack PDUs
- produce bind_ack result lists matching requested presentation contexts
- negotiate BTFN as `negotiate_ack`
- add sec trailers to authenticated DCE/RPC responses when request auth length is nonzero
- distinguish DCE management interface short stubs from NSPI or EMSMDB operation stubs
- return RFRI, NSPI, and EMSMDB opnum responses on the correct context

Likely MAPI over HTTP fixes:

- ensure missing auth gets the expected challenge
- return `application/octet-stream` for MAPI payloads
- maintain authenticated session context by request ID/session cookie
- map mailbox, address book, folder, message, contact, calendar, task, and submission behavior to canonical LPE state
- avoid publishing MAPI endpoints through autodiscover unless the implementation path is enabled and testable

Success criteria:

- RCA endpoint pings succeed for `:6001`, `:6002`, and `:6004`
- referral operation returns a valid address book server
- address book Check Name resolves the test user
- mail store endpoint ping and mailbox operation do not time out
- MAPI over HTTP bind/connect/disconnect and basic execute calls return valid MAPI responses

Suggested tests:

```bash
cargo test -p lpe-exchange rpc_proxy_
cargo test -p lpe-exchange mapi_over_http
cargo test -p lpe-exchange resolve_names
cargo test -p lpe-exchange
```

## Efficient RCA Iteration Loop

For each RCA run:

1. Save the RCA error block exactly.
2. Save LPE logs from the matching time window.
3. Identify whether the deployed behavior matches source.
4. If behavior differs from source, stop and fix deployment first.
5. If behavior matches source, check Microsoft docs for that operation.
6. Create one focused regression test that reproduces the observed packet or SOAP/WBXML shape.
7. Patch only the protocol behavior needed for that RCA step.
8. Run focused tests.
9. Run the full affected crate tests.
10. Tell the operator the exact expected log change for the next RCA run.

## Final Answer Template

Use this shape:

```text
I checked Microsoft documentation first:
- <doc link 1>
- <doc link 2>

RCA failed at <step>. The LPE logs show <specific observation>. That means <classification>.

Changed:
- <file 1>: <short change>
- <file 2>: <short change/test>

Verified:
- <command>: <result>
- <command>: <result>

On the next RCA run, expect <specific log delta>. If it still shows <old value>, check <deployment drift / next likely issue>.
```

