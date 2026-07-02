# LPE Maintenance And Outlook Architecture Re-Audit - 2026-07-02

## Scope

This re-audit checks the current repository after the June 27 maintenance and
Outlook architecture recommendations were implemented.

The audit verifies:

- whether the previous documentation contradictions were resolved
- whether production source files were split below the agreed 1,500-line target
- whether shared helper duplication was centralized
- whether Outlook/Exchange parity remains the governing objective
- whether the new maintenance controls and parity roadmaps are useful

Read before auditing:

- `ARCHITECTURE.md`
- `docs/architecture/initial-architecture.md`
- `LICENSE.md`
- `AGENTS.md`
- `docs/architecture/ews-mapi-mvp.md`
- `docs/architecture/mapi-over-http-implementation-plan.md`
- `docs/architecture/mapi-full-object-support-execution.md`
- `docs/architecture/client-autoconfiguration.md`
- `docs/architecture/edge-and-protocol-exposure.md`
- `docs/architecture/outlook-exchange-parity-roadmap.md`
- `docs/architecture/maintenance-refactor-backlog.md`
- `docs/i18n.md`

## Executive Assessment

The recommendations from the June audit are substantially complete.

The repository is now in a much better architectural state:

- High-level docs still identify full Outlook support as an explicit release
  goal.
- MAPI over HTTP remains the primary Outlook desktop Exchange-account route.
- ActiveSync is still correctly limited to mobile/native ActiveSync clients.
- Public MAPI autodiscover remains gated by local harness, Microsoft RCA, and
  real Outlook 2016/2019 cached-mode evidence.
- The old durable contradiction that `lpe-exchange` was "not a complete
  Exchange server" has been replaced with accurate readiness language: full
  Outlook functionality is the target, current compatibility is incomplete and
  release-gated, and protocol-local canonical state remains forbidden.
- `docs/i18n.md` no longer says MAPI over HTTP is out of scope; it now says
  MAPI over HTTP is part of the Outlook desktop objective but outside the
  internationalized-mailbox plan.
- Shared helpers now exist in `lpe-domain` for normalization, civil time,
  crypto, and mail formatting.
- The worst production source files in `lpe-exchange` and `LPE-CT` have been
  split into focused modules.
- `tools/check_oversized_sources.py` and `tools/check_repository.py` now give a
  practical process control for future growth.

The remaining issues are real but narrower:

1. `crates/lpe-storage/sql/schema.sql` is still the only production source file
   above the 1,500-line threshold reported by the checker.
2. Test files remain very large, especially JMAP tests and Exchange/MAPI test
   modules. Production code is much better; test maintainability is now the
   largest structural gap.
3. `docs/architecture/maintenance-refactor-backlog.md` is now over 11,000
   lines. It is useful evidence, but it has become a logbook and should be
   split or summarized.
4. Some compatibility wrapper functions remain around shared helpers. They are
   harmless delegates today, but future code should import `lpe-domain`
   directly where crate boundaries allow it.
5. Several Outlook-visible behaviors are still intentionally incomplete or
   gated. This is acceptable only because they are tracked as parity gaps, not
   permanent refusals.

## Documentation Result

No current top-level instruction was found that contradicts the full Outlook
objective.

Resolved from the June audit:

- `docs/architecture/ews-mapi-mvp.md` now says full Outlook functionality is
  the target and frames incomplete Exchange behavior as release-gated work.
- `docs/i18n.md` no longer excludes MAPI over HTTP from the product objective.
- `docs/architecture/outlook-exchange-parity-roadmap.md` tracks unsupported or
  incomplete Outlook/Exchange behaviors as gaps with canonical model and
  evidence requirements.
- `docs/architecture/maintenance-refactor-backlog.md` records refactor status,
  verification commands, and remaining work.
- `AGENTS.md` now contains a durable source-size rule and a line-count scan
  recommendation.

Still important:

- The docs correctly continue to forbid protocol-local canonical mailbox,
  `Sent`, `Outbox`, rights, collaboration, or LPE-CT mailbox state.
- The public MAPI gate should not be weakened. It remains the right safety
  boundary.

## Source Size Result

`python tools/check_oversized_sources.py` output:

```text
Checked 583 production source files; threshold is 1500 lines.
Oversized production source files:
  3455  crates/lpe-storage/sql/schema.sql
Warning only. Pass --fail to make oversized files fail the check.
```

This is a strong improvement over the June audit. The previous hotspots are now
below the production threshold or split into focused modules:

| Previous hotspot | Current status |
| --- | --- |
| `crates/lpe-exchange/src/mapi/dispatch.rs` | 1,423 counted lines by raw scan; now a hub plus focused `mapi/dispatch/*` modules. |
| `crates/lpe-exchange/src/mapi/tables.rs` | Below threshold; table code split into `mapi/tables/*`. |
| `crates/lpe-exchange/src/mapi/properties.rs` | 1,498 lines, barely below threshold; split into `mapi/properties/*`. |
| `crates/lpe-exchange/src/mapi/rop.rs` | 1,477 lines; split into `mapi/rop/*`. |
| `crates/lpe-exchange/src/service.rs` | 1,018 lines; split into EWS, MAPI HTTP, RPC proxy, and HTTP utility modules. |
| `LPE-CT/src/smtp.rs` | 1,214 lines; split into SMTP policy, auth, queue, quarantine, DNS, delivery, trace, and related modules. |
| `LPE-CT/src/main.rs` | Raw scan reported 1,492 lines, below threshold; route/config/readiness/auth extraction landed. |
| `tools/rca_outlook_connectivity_check.py` | 1,454 lines; helper modules created under `tools/rca_outlook/`. |

`schema.sql` should either become a documented dense-schema exception or be
split by schema domain with tooling that preserves install/update semantics.

## Test Size Result

`python tools/check_oversized_sources.py --include-tests` still reports many
oversized test files:

| Lines | File |
| ---: | --- |
| 15,013 | `crates/lpe-jmap/src/tests.rs` |
| 10,201 | `crates/lpe-exchange/src/tests/mod.rs` |
| 9,479 | `crates/lpe-exchange/src/tests/mapi_over_http/sync.rs` |
| 9,370 | `crates/lpe-exchange/src/tests/ews.rs` |
| 8,132 | `crates/lpe-exchange/src/mapi/tables/tests.rs` |
| 6,842 | `crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs` |
| 6,257 | `crates/lpe-activesync/src/tests.rs` |
| 6,013 | `crates/lpe-exchange/src/mapi/properties/tests.rs` |
| 5,799 | `crates/lpe-exchange/src/tests/mapi_over_http/properties.rs` |
| 5,410 | `crates/lpe-storage/tests/runtime_schema_drift.rs` |

The test split work improved the original 49k-line MAPI file, but many scenario
modules are still too large for fast review. Next maintenance work should
target tests, not more production reshuffling.

## Shared Helper Result

The helper centralization recommendation was largely completed.

New shared modules:

- `crates/lpe-domain/src/civil_time.rs`
- `crates/lpe-domain/src/crypto.rs`
- `crates/lpe-domain/src/mail_format.rs`
- `crates/lpe-domain/src/normalization.rs`

`rg` now shows canonical helper definitions in `lpe-domain`. Remaining
duplicates are mostly compatibility wrappers:

- `crates/lpe-storage/src/util.rs` delegates normalization and `sha256_hex` to
  `lpe-domain`.
- `crates/lpe-mail-auth/src/auth.rs` delegates `normalize_login_name` to
  `lpe-domain`.
- `crates/lpe-imap/src/render.rs::month_name` is IMAP date parsing, not the
  same responsibility as RFC 5322 mail formatting.

No urgent cleanup is required here. Future code should prefer direct
`lpe-domain` imports where that does not create churn.

## Outlook Parity Status

The product objective remains intact but not complete.

The current docs correctly state:

- full Outlook functionality is the target
- current Exchange compatibility is incomplete and gated
- MAPI over HTTP is the Outlook desktop route
- EWS is the active Exchange compatibility adapter
- all client layers must converge on canonical LPE state
- no adapter may implement parallel `Sent` or `Outbox`
- LPE-CT remains the edge/sorting center and must not hold canonical mailbox or
  collaboration state

Remaining Outlook-visible parity gaps are now tracked rather than hidden:

- cross-process MAPI session replay and load-balanced failover
- full FastTransfer/ICS parity
- raw Exchange marker/subobject destination streams
- full Exchange property-bag breadth
- NSPI mutation/link-table and full address-book template behavior
- Exchange rule blobs, client-only rules, delegate rule templates, and deferred
  action messages
- full search-folder/Common Views parity
- notification payload/replay parity
- full Recoverable Items/dumpster parity
- public-folder replication and broader per-user/public-folder semantics
- EWS feature-family gaps such as message tracking, persona/UCS/IM, managed
  folders, mail apps, compliance, and full user configuration

This is acceptable because each gap is framed as requiring a canonical model,
tests, and real-client evidence. None should be described as a permanent refusal
unless the product objective is deliberately narrowed.

## Placeholder And Stub Result

The previous `stub-local` AI runtime concern appears resolved. `rg` no longer
finds `stub-local` in source.

Remaining placeholder references are mostly:

- tests
- documentation
- weak-password placeholder rejection
- Outlook associated-configuration placeholder suppression

The Outlook placeholder suppression functions are still present but are now
better localized under `mapi_store`, `mapi/tables`, and
`mapi/dispatch/associated_config`. They appear to be compatibility filters, not
demo/runtime placeholder content.

## Verification Run

Commands run during this re-audit:

```text
python tools/check_oversized_sources.py
python tools/check_oversized_sources.py --include-tests
python tools/check_repository.py
cargo test -p lpe-domain --quiet
cargo test -p lpe-exchange ews --quiet
cargo test -p lpe-storage schema_contract --quiet
$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange --quiet
cargo test --quiet
python -m py_compile tools/rca_outlook_connectivity_check.py tools/rca_outlook/cli.py tools/rca_outlook/http.py tools/rca_outlook/ews.py tools/rca_outlook/mapi.py
```

Results:

- `check_oversized_sources.py`: passed warning mode; reports only
  `schema.sql` as oversized production source.
- `check_oversized_sources.py --include-tests`: passed warning mode; reports
  oversized test files listed above.
- `check_repository.py`: passed warning mode; same production oversized-source
  warning for `schema.sql`.
- `lpe-domain`: 34 tests passed.
- focused `lpe-exchange ews`: 215 tests passed.
- `lpe-storage schema_contract`: 67 tests passed.
- full `lpe-exchange`: 1,594 tests passed.
- `LPE-CT`: 85 tests passed, 19 ignored.
- RCA Python modules compiled successfully.

## Recommendations

1. Decide whether `schema.sql` is a documented dense-schema exception or split
   it mechanically by schema domain. Do not do a semantic schema refactor just
   to satisfy line count.
2. Start a test-file split pass. Highest priority:
   - `crates/lpe-jmap/src/tests.rs`
   - `crates/lpe-exchange/src/tests/mod.rs`
   - `crates/lpe-exchange/src/tests/mapi_over_http/sync.rs`
   - `crates/lpe-exchange/src/tests/ews.rs`
3. Split or summarize `docs/architecture/maintenance-refactor-backlog.md`.
   Keep detailed dated logs if useful, but move completed execution history to
   per-area audit appendices or release notes.
4. Keep `tools/check_oversized_sources.py` in warning mode until `schema.sql`
   is handled; then consider enabling `--fail` in CI for production source.
5. Continue treating all remaining unsupported Outlook behavior as parity debt
   unless Microsoft documentation, real Outlook traces, or canonical LPE
   security/state requirements prove LPE should not implement it.
6. Do not loosen the MAPI autodiscover gate. The current gate is still the
   correct boundary between implemented code and a public Outlook compatibility
   claim.
