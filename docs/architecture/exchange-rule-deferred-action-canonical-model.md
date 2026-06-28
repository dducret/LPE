# Exchange Rule And Deferred-Action Canonical Model

This note defines the current rules/deferred-action boundary for EWS and MAPI
compatibility. It is documentation-only and does not widen rule execution.

## Current Behavior

Canonical mailbox rules are Sieve-backed. The current shared state is
`sieve_scripts`, active script selection, canonical rule projections, audit, and
mail change-log state.

The implemented protocol projections are bounded:

- ManageSieve uploads, lists, activates, retrieves, renames, and deletes
  canonical Sieve scripts.
- JMAP `VacationResponse/*` reads and writes a generated canonical vacation
  Sieve script.
- JMAP private `Rule/get`, `Rule/query`, `Rule/changes`, and
  `Rule/queryChanges` read canonical rule projections.
- EWS `GetInboxRules` projects canonical Sieve-backed rules.
- EWS `UpdateInboxRules` creates, updates, and deletes only rule shapes that
  can be represented safely as generated canonical Sieve.
- MAPI `RopGetRulesTable` projects canonical Sieve-backed rules.
- MAPI `RopModifyRules` accepts only generated bounded provider data that maps
  to canonical Sieve actions.

The current bounded mutation surface covers:

- `always`, subject-contains, and from-contains style predicates where the
  protocol adapter has a deterministic generated representation;
- move/fileinto;
- delete/discard;
- forward/redirect through canonical submission semantics;
- mark-read as a bounded canonical rule action marker;
- stop-processing.

## Rejected Exchange-Only State

The following remain rejected because accepting them as opaque Exchange data
would either activate unsafe behavior or create a MAPI/EWS-local rule store:

- Exchange rule condition/action blobs;
- `IPM.ExtendedRule.Message` or RuleOrganizer rows as active rule state;
- client-only rules;
- delegate rule templates;
- provider-specific predicates;
- deferred-action provider data;
- EWS `DeferredActionMessage` payloads;
- MAPI `RopUpdateDeferredActionMessages` uploads.

Rejected shapes must return parseable protocol errors and must not write,
activate, delete, or reorder Sieve scripts. They also must not persist as
durable Outlook compatibility metadata unless a future architecture update
explicitly classifies them as inert client configuration rather than active rule
behavior.

## Boundary

Rules have three separate concerns:

| Concern | Owner | Rule |
| --- | --- | --- |
| Canonical execution | `lpe-core` / storage-backed Sieve service | Owns script lifecycle, active selection, generated rule mutation, vacation projection, audit, and rule change-log state. |
| Protocol projection | JMAP, EWS, MAPI, ManageSieve adapters | Parse protocol payloads, map supported shapes into canonical service inputs, and map errors back to protocol responses. |
| Outlook compatibility metadata | `lpe-exchange` only when documented as inert metadata | May preserve bounded configuration data only when it is not active rule behavior and does not shadow canonical Sieve state. |

No protocol adapter should independently decide whether an Exchange rule blob is
safe to execute. If a behavior cannot be represented by the canonical rule
service, it remains unsupported.

## Canonical Model Needed For Wider Support

Before supporting Exchange rule blobs or deferred actions, LPE needs a canonical
model for:

- a normalized rule AST independent of Sieve text;
- condition/action coverage, including ordering, stop-processing, exceptions,
  recipient/sender/address-book predicates, category predicates, and folder
  targets;
- deferred action semantics, including when an action is evaluated, retried,
  cancelled, or audited;
- sender-right and submission behavior for forward/redirect actions;
- delegate rule ownership and visibility;
- inactive or client-only metadata that Outlook may round-trip without server
  execution;
- migration to and from generated Sieve where possible;
- cross-protocol change events and conflict behavior.

Sieve can remain the execution backend for mappable server-side rules, but the
canonical model must decide what a rule means before any Exchange-specific blob
is accepted.

## Tests And Evidence

Existing evidence covers the bounded model:

- `inbox_rules_project_and_update_canonical_sieve_rules`
- `update_inbox_rules_rejects_exchange_only_rule_shapes_without_side_effects`
- `mapi_over_http_get_rules_table_projects_canonical_sieve_rules`
- `mapi_over_http_modify_rules_writes_bounded_canonical_sieve_rule`
- `mapi_over_http_modify_rules_accepts_bounded_sieve_actions`
- `mapi_over_http_modify_rules_rejects_exchange_rule_blobs`
- `mapi_over_http_update_deferred_action_messages_rejects_without_sieve_side_effect`
- ManageSieve script lifecycle tests
- JMAP vacation response tests

Before widening support, add tests proving:

- unsupported Exchange blobs do not activate Sieve or create protocol-local
  active rules;
- supported rules produce the same canonical rule state through EWS, MAPI, JMAP,
  and ManageSieve where those protocols expose the concept;
- forwarded/redirected messages use canonical submission and LPE-CT relay;
- delegate and client-only rules have explicit canonical ownership and
  visibility semantics;
- real Outlook 2016 and Outlook 2019 traces require the widened behavior.

## Public MAPI Autodiscover Impact

`RopGetRulesTable` projection and safe rejection of unsupported rule/deferred
action uploads are part of Outlook profile and cached-mode resilience.

Full Exchange rule/deferred-action parity does not block public MAPI
autodiscover unless real Outlook evidence shows profile creation, cached-mode
sync, send, reconnect, or shutdown requires accepting those rule uploads for the
supported Outlook versions.
