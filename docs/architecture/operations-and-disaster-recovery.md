# Operations And Disaster Recovery

This document defines the supported operational playbooks for `LPE` and `LPE-CT`.

It focuses on four areas:

- backup and restore boundaries
- replaceable `LPE-CT` node operations
- schema-migration safety checks
- explicit rollback constraints per release

The rules here are aligned with the architecture split:

- `LPE` keeps canonical mailbox, collaboration, rights, and user-visible state in `PostgreSQL`
- `LPE-CT` keeps perimeter transport custody in its spool plus optional technical indexes in its own private database
- operators must never treat `LPE-CT` technical state as canonical mailbox truth

## Operating principles

The operational model is intentionally conservative:

- protect canonical state first
- keep transport custody reconstructable
- prefer node replacement over in-place repair for `LPE-CT`
- prefer reversible release steps over partial upgrades
- treat database schema changes as release gates, not as background hygiene

Every release must declare:

- required backup scope
- whether the schema change is additive-only, expand-and-contract, or irreversible
- whether mixed-version operation is allowed during rollout
- whether rollback is code-only or requires data restore

## Backup boundaries

### Core `LPE`

The minimum protected backup scope for the core is:

- canonical `PostgreSQL` database
- configuration files and secrets required to start the active node
- deployed application revision or packaged build artifact
- reverse-proxy and TLS configuration required to republish the service

The database backup is the authoritative backup for:

- mailboxes
- canonical message bodies and metadata
- contacts
- calendars
- tasks
- rights, delegation, and sharing state
- canonical outbound queue state
- audit and protocol sync state stored in the core database

The core node itself must be considered replaceable as long as the database and required configuration are preserved.

### `LPE-CT`

The minimum protected backup scope for the sorting center is:

- `/var/spool/lpe-ct`
- `state.json` or equivalent local bootstrap state
- local `LPE-CT` configuration and secrets
- private `LPE-CT` technical database when enabled

The spool backup is the authoritative backup for:

- inbound queue custody
- outbound queue custody after handoff into `LPE-CT`
- quarantine payload custody
- held and deferred transport artifacts
- `DSN` and replay-oriented trace artifacts

The private `LPE-CT` database is a technical acceleration layer. It should be backed up because it improves recovery time, but it remains rebuildable from:

- current spool contents
- current quarantine contents
- ongoing perimeter traffic
- bounded retained technical history

## Backup cadence guidance

The default operating target is:

- regular physical or logical `PostgreSQL` backups with tested restore verification
- frequent enough spool snapshots or replicated copies that transport-custody loss stays within the declared operator risk budget
- separate retention for quarantine payloads, because incident review often outlives ordinary queue retention

The exact schedule is deployment-specific, but every installation must document:

- recovery point objective for canonical `LPE` data
- recovery point objective for `LPE-CT` spool custody
- retention duration for quarantine payloads and technical traces

## Restore playbooks

### Core database restore

Use this playbook when canonical `LPE` state must be recovered:

1. stop writes to the current active core endpoint
2. verify the selected backup timestamp and release compatibility
3. restore `PostgreSQL` onto the target primary
4. apply only the application version whose schema contract matches that backup
5. keep all non-active core nodes in `standby` or `maintenance`
6. restore secrets and reverse-proxy configuration
7. promote the intended active node and verify `/health/ready`
8. validate protocol publication for `JMAP`, `IMAP`, `ActiveSync`, `DAV`, and webmail
9. verify outbound queue behavior before reopening user traffic

After restore, operators must explicitly validate:

- tenant login paths
- mailbox read consistency
- outbound queue state
- push and long-poll behavior

### `LPE-CT` spool restore

Use this playbook when transport custody must be recovered:

1. keep the target `LPE-CT` node in `maintenance`
2. restore `/var/spool/lpe-ct` and local configuration
3. restore the local technical database only if it materially reduces recovery time
4. run spool inventory and consistency tooling before re-enabling traffic
5. inspect `deferred`, `held`, `quarantine`, and `outbound` queues separately
6. replay only items whose intended next step is still valid for the current release
7. move the node to `standby` first, then to `active` only after validation

If the local `LPE-CT` database is unavailable but the spool is intact, operators may rebuild technical indexes and return the node to service. If the spool is lost, transport custody for those items is lost even if the local database survives.

## `LPE-CT` node replacement

`LPE-CT` nodes must remain operationally replaceable.

The supported replacement model is:

- install a fresh node from the declared release artifact
- restore or attach the required configuration and secrets
- attach the expected private technical database only if that deployment topology uses one
- restore spool custody only when this specific node owns surviving queue artifacts
- otherwise join the node empty and let it warm from current traffic plus shared technical state

### Planned replacement

For planned replacement of an `LPE-CT` node:

1. drain or remove it from active publication
2. set its role to `maintenance`
3. verify no new SMTP ingress or submission traffic lands on the node
4. inventory remaining spool items
5. replay, transfer, or manually resolve retained `deferred`, `held`, and `quarantine` items
6. install the replacement node with the same release and configuration baseline
7. validate readiness, relay reachability, and core delivery reachability
8. return it first as `standby` or as a horizontal spare, then to active publication if needed

### Unplanned replacement

For host loss or unrecoverable node failure:

1. remove the failed node from `MX`, NAT, VIP, or load-balancer publication
2. activate a healthy replacement node
3. verify SMTP ingress, submission, and core final delivery on the replacement
4. if the failed node later returns, inspect its spool before reusing any artifacts
5. import only safe recoverable queue items; keep quarantine under manual review

The replacement objective is continuity of perimeter service, not perfect preservation of every in-flight spool item on a lost host.

## Upgrade safety checks

Every release must ship with an explicit pre-upgrade checklist.

The minimum checklist is:

1. confirm the current and target versions
2. confirm whether the release contains schema migrations
3. confirm recent validated backups for core `PostgreSQL` and required `LPE-CT` spool scope
4. confirm available rollback path for the target version
5. confirm the target release notes declare mixed-version compatibility or explicitly forbid it
6. confirm all standby or spare nodes are still on the pre-upgrade role expected for the rollout
7. confirm enough free disk for database migration temp space, spool growth, and package staging

### Schema migration safety

Schema migration safety is release-blocking.

Before applying a release with core schema changes:

1. review whether the migration is additive-only, expand-and-contract, or irreversible
2. verify the migration has been tested against a production-like backup or fixture set
3. verify the application build and migration scripts come from the same release
4. verify no older active node will continue writing incompatible data after the migration
5. verify observability and alerting are in place for migration failure, long lock time, and replica lag

The supported rollout preference is:

- additive schema first
- code that tolerates both old and new shapes during transition
- destructive cleanup only in a later release once rollback is no longer required

Direct destructive migrations are allowed only when the release notes declare rollback as restore-only and operators accept that constraint before rollout.

## Rollback constraints per release

Every release must declare one rollback class:

### Class A: code-only rollback

Allowed when:

- no schema change occurred, or
- the schema change is strictly additive and unused by the downgraded code

Operational rule:

- stop the new binaries
- redeploy the previous binaries
- keep the same restored database and spool state

### Class B: code-plus-forward-compatible data rollback

Allowed when:

- the schema changed additively
- the previous version can still operate safely against the post-upgrade data shape
- no irreversible background job has rewritten canonical data into a downgrade-incompatible form

Operational rule:

- downgrade binaries only after confirming the previous release’s compatibility statement
- keep any new features or writers disabled if the older release cannot understand them fully

### Class C: restore-required rollback

Required when:

- the schema migration is destructive, contractive, or semantically irreversible
- queue semantics changed incompatibly
- background jobs rewrote canonical data

Operational rule:

- stop traffic
- restore the declared compatible `PostgreSQL` backup
- restore required `LPE-CT` spool scope if the release changed transport custody semantics
- redeploy the previous release everywhere before reopening traffic

No release may omit its rollback class.

## Mixed-version constraints

The default rule is conservative:

- core `LPE` nodes are not assumed to be mixed-version write-compatible unless the release explicitly says so
- `LPE-CT` nodes may be briefly mixed-version only when the HTTP integration contract and spool semantics are unchanged
- protocol adapters that expose long-lived sessions must prefer reconnect over silent mixed-version session continuation

That implies:

- upgrade the passive core first only when the target release is confirmed compatible with the active core database state
- fail over the core only after schema and release checks pass
- keep incompatible `LPE-CT` nodes out of active publication until all edge nodes reach the required contract version

## Release playbook shape

Each release note affecting operations must include these sections:

- backup scope
- migration type
- mixed-version support
- rollback class
- operator validation steps

The minimum post-upgrade validation set is:

- core `/health/ready`
- `LPE-CT` `/health/ready`
- one `JMAP` request path
- one `IMAP` login and mailbox selection
- one `ActiveSync` long-poll or sync path
- one outbound submission path
- one inbound delivery path
- queue and quarantine dashboards return current data

## Disaster scenarios that must be covered

Operators must maintain documented procedures for at least:

- loss of the active core node
- loss of the active `PostgreSQL` primary
- loss of the active `LPE-CT` node
- return of a failed `LPE-CT` node with surviving spool contents
- accidental bad release requiring rollback
- schema migration failure mid-upgrade

The objective is not zero-risk automation. The objective is a bounded, documented, testable recovery path for the supported `LPE` architecture.
