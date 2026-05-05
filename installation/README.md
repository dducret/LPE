# Installation

These installation instructions are aligned with the current repository schema `0.1.8`.

Legacy updates from schemas that predate the current schema metadata are not supported. Fresh installs still initialize the database from the canonical schema, but routine updates must preserve existing data and apply only explicit migrations.

### Debian Trixie

The `debian-trixie` directory prepares a source installation of `LPE` from:

- `https://github.com/dducret/LPE`

Default product directory:

- `/opt/lpe`

Temporary bootstrap directory used in the examples:

- `~/LPE-bootstrap`

Important:

- `~/LPE-bootstrap` is only used to fetch and run the scripts on a bare Debian server
- the actual product installation is then performed by `install-lpe.sh` into `/opt/lpe`
- after installation, the product-side reference checkout is `/opt/lpe/src`
- the bootstrap checkout can be limited to `installation/debian-trixie` using Git sparse checkout
- the product checkout in `/opt/lpe/src` must remain complete because the Rust build needs the full workspace

Operating assumptions:

- minimal Debian Trixie server
- no desktop environment
- root shell or an account with `sudo`
- no local dependency assumed at the start except network access

Path conventions:

- installed product files for `LPE` live under `/opt/lpe`
- installed product files for `LPE-CT` live under `/opt/lpe-ct`
- bundled executables such as `magika` and the default `takeri` CLI live under `/opt/lpe/bin` or `/opt/lpe-ct/bin`
- bundled source/vendor content such as the synchronized `takeri` checkout lives under `/opt/lpe-ct/vendor`
- mutable `LPE` state lives under `/var/lib/lpe`
- mutable `LPE-CT` state and technical metadata live under `/var/lib/lpe-ct`
- mutable `LPE-CT` transport and quarantine spool data live under `/var/spool/lpe-ct`
- the default indexed `LPE-CT` technical state lives in a private dedicated `PostgreSQL` database, typically `postgres://lpe_ct@127.0.0.1:5432/lpe_ct`
- configuration lives under `/etc/lpe` and `/etc/lpe-ct`
- `bayespam` is not installed as a separate package root under `/opt`; its mutable corpus and technical state remain part of `LPE-CT` runtime state under `/var`

For a separate sorting server in the `DMZ`, use `LPE-CT/installation/debian-trixie` instead. That subdirectory installs a distinct component into `/opt/lpe-ct` with its own management UI, without exposing the core back office on the DMZ server, also provisions a pinned `Magika` CLI binary in `/opt/lpe-ct/bin/magika` for inbound SMTP validation, performs a Git-based sparse synchronization of `takeri` from `https://github.com/AnimeForLife191/Shuhari-CyberForge.git` before building `/opt/lpe-ct/bin/Shuhari-CyberForge-CLI` as the default antivirus provider, and now provisions the default private `LPE-CT` PostgreSQL store for dashboard management state, greylisting, reputation, `bayespam`, throttling, and quarantine metadata.

The `LPE-CT` scripts also install the SMTP ingress listener on `25`, publish the HTTPS edge through `nginx` on `443`, redirect plain `HTTP` on `80` to `HTTPS`, configure authenticated implicit-TLS client submission on `465`, configure the IMAPS TLS proxy on `993`, create the full runtime spool layout in `/var/spool/lpe-ct`, and provide these validation scripts:

The generated `LPE-CT` `nginx` site also re-exposes the core client publication
routes on the public HTTPS edge: `/Microsoft-Server-ActiveSync`, `/mapi/`,
`/autodiscover/`, `/Autodiscover/`, `/autoconfig/`, and
`/.well-known/autoconfig/`. ActiveSync and MAPI use long proxy timeouts so
mobile and Outlook long-poll requests are not cut off by the edge proxy. The
`install-lpe-ct.sh` and `update-lpe-ct.sh` scripts validate the rendered nginx
publication settings, while `check-lpe-ct.sh` and `test-lpe-ct-edge-ports.sh`
validate the live autodiscover and ActiveSync responses through the public edge.
The same generated `HTTPS` site now emits baseline security headers:
`Strict-Transport-Security: max-age=31536000`, `X-Content-Type-Options:
nosniff`, `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`,
`Content-Security-Policy: frame-ancestors 'none'`, and a restrictive
`Permissions-Policy`. HSTS intentionally omits `includeSubDomains` by default;
only add it locally when every subdomain under the published domain is covered
by the same HTTPS guarantee. The CSP is deliberately limited to frame ancestry
until script/style/connect directives can be validated against the webmail,
administration UI, `JMAP`, `ActiveSync`, `EWS`, `MAPI`, and autodiscover flows.

They also install `/opt/lpe-ct/bin/lpe-ct-host-action` with a narrow sudoers policy for management-console host maintenance actions. The `lpe-ct` service still runs as the non-root `lpe-ct` user, but NTP changes, manual time sync, `apt update && apt upgrade -y`, restart, and shutdown are delegated to that root-owned helper. Existing nodes must rerun `LPE-CT/installation/debian-trixie/update-lpe-ct.sh` to receive the helper, sudoers file, and refreshed service hardening settings.

For the first `active/passive` `DMZ` deployment step, `LPE-CT/installation/debian-trixie` also provides `check-lpe-ct-ready.sh`, `lpe-ct-ha-set-role.sh`, and `keepalived-lpe-ct.conf.example`.
It now also provides `test-ha-lpe-ct-active-passive.sh`, `lpe-ct-spool-recover.sh`, and `test-lpe-ct-spool-recovery.sh` for traffic gating and spool return-to-service validation.

The functional `LPE` / `LPE-CT` integration also requires aligned `LPE_CT_CORE_DELIVERY_BASE_URL`, `LPE_CT_API_BASE_URL`, and `LPE_INTEGRATION_SHARED_SECRET` values across the two nodes. `LPE_CT_CORE_DELIVERY_BASE_URL` points from `LPE-CT` to the core `LPE` HTTP listener, default port `8080`, and is used for `/internal/lpe-ct/inbound-deliveries`, `/internal/lpe-ct/recipient-verification`, `/internal/lpe-ct/submission-auth`, and `/internal/lpe-ct/submissions`. `LPE_CT_API_BASE_URL` points from the `LPE` outbound worker to the `LPE-CT` management/API listener, default port `8380`, and is used for `/api/v1/integration/outbound-messages`. `LPE_INTEGRATION_SHARED_SECRET` is mandatory for `LPE <-> LPE-CT` bridge traffic, must stay out of public interfaces, and must be set to a strong non-trivial value of at least `32` characters. On `LPE-CT`, a missing or weak value now leaves the management UI reachable but reports the bridge as degraded until the secret is fixed. The contract is documented in `docs/architecture/lpe-ct-integration.md`.

Accepted inbound domains for the public `LPE-CT` SMTP listener are not environment variables. After the `LPE-CT` service is running, add and verify each domain in the management console under `System Setup -> Mail relay -> Domains`; the active domain list is stored in the private `LPE-CT` PostgreSQL dashboard state.

- `test-local-lpe-ct.sh` from the `LPE-CT` server
- `test-from-lpe.sh` from the LAN or core server to verify the signed canonical `LPE -> LPE-CT` outbound handoff API
- `test-from-internet.sh` from an external machine
- `test-lpe-ct-edge-ports.sh` from the `LPE-CT` server to verify listeners on `25`, `443`, `465`, and `993`
- `test-lpe-ct-core-bridge.sh` from the `LPE-CT` server to verify the signed `LPE-CT -> LPE` recipient-verification bridge
- `test-lpe-imap-listener.sh` from the core `LPE` server to verify the internal `IMAP` listener used by the `LPE-CT` `993` proxy
- `check-lpe-env.sh` from the core `LPE` server to list active variables that are present in `lpe.env.example` but missing from `/etc/lpe/lpe.env`; `update-lpe.sh` runs this check automatically in warning mode
- `check-lpe-ct-env.sh` from the `LPE-CT` server to list active variables that are present in `lpe-ct.env.example` but missing from `/etc/lpe-ct/lpe-ct.env`; `update-lpe-ct.sh` runs this check automatically in warning mode
- `test-antivirus-lpe-ct.sh` from the `LPE-CT` server to validate quarantine on an `EICAR` attachment

The `LPE-CT` test scripts that inject mail require real mailbox addresses through environment variables. For example, run the antivirus check as:

```bash
cd /opt/lpe/src/LPE-CT/installation/debian-trixie
SENDER=postmaster@example.net \
RECIPIENT=user@example.com \
./test-antivirus-lpe-ct.sh
```

For `test-antivirus-lpe-ct.sh`, the SMTP final reply can be either a quarantine `250` or a perimeter-policy `554` depending on the other edge checks that apply to the chosen sender. The validation target is the quarantined trace written under `/var/spool/lpe-ct/quarantine`, and the script now verifies that retained trace instead of assuming one specific SMTP reply text.

To inspect missing `LPE-CT` environment variables after an update, run:

```bash
cd /opt/lpe/src/LPE-CT/installation/debian-trixie
sudo ./check-lpe-ct-env.sh
```

To append missing variables with their example defaults, run:

```bash
sudo ./check-lpe-ct-env.sh --append-missing
```

Review appended values before restarting services, especially secrets, hostnames,
database URLs, TLS paths, and bridge URLs.

To inspect missing core `LPE` environment variables after an update, run:

```bash
cd /opt/lpe/src/installation/debian-trixie
sudo ./check-lpe-env.sh
```

To append missing variables with their example defaults, run:

```bash
sudo ./check-lpe-env.sh --append-missing
```

Review appended values before restarting services, especially secrets,
database URLs, public hostnames, bridge URLs, and bind addresses.

To test the canonical outbound handoff from `LPE` to `LPE-CT`, run this from the
core `LPE` server:

```bash
cd /opt/lpe/src/installation/debian-trixie
sudo ./test-from-lpe.sh
```

The script reads `LPE_CT_API_BASE_URL` and `LPE_INTEGRATION_SHARED_SECRET` from
`/etc/lpe/lpe.env`, signs a `POST
${LPE_CT_API_BASE_URL}/api/v1/integration/outbound-messages` request, and checks
the `LPE-CT` response. By default it uses reserved `example.test` addresses. For
a real relay-path test, pass real values:

```bash
sudo SENDER=user@example.com RECIPIENT=external@example.net ./test-from-lpe.sh
```

### LPE-CT Public TLS Certificate

Before starting the public `LPE-CT` edge on `443`, `465`, and `993`, install a
certificate covering the public `LPE-CT` hostname, for example
`mx.example.com`, under `/etc/lpe-ct/tls`:

```bash
install -d -m 0750 -o root -g lpe-ct /etc/lpe-ct/tls
install -m 0640 -o root -g lpe-ct fullchain.pem /etc/lpe-ct/tls/fullchain.pem
install -m 0640 -o root -g lpe-ct privkey.pem /etc/lpe-ct/tls/privkey.pem
```

The Rust `lpe-ct` service must read this certificate and private key for `465`
and `993`. If the service logs `unable to open certificate ... Permission
denied`, repair the ownership and mode with:

```bash
chown root:lpe-ct /etc/lpe-ct/tls /etc/lpe-ct/tls/fullchain.pem /etc/lpe-ct/tls/privkey.pem
chmod 0750 /etc/lpe-ct/tls
chmod 0640 /etc/lpe-ct/tls/fullchain.pem /etc/lpe-ct/tls/privkey.pem
sudo -u lpe-ct test -r /etc/lpe-ct/tls/fullchain.pem
sudo -u lpe-ct test -r /etc/lpe-ct/tls/privkey.pem
```

Configure the same certificate paths for the three TLS surfaces unless you
intentionally split certificates:

```bash
LPE_CT_PUBLIC_TLS_CERT_PATH=/etc/lpe-ct/tls/fullchain.pem
LPE_CT_PUBLIC_TLS_KEY_PATH=/etc/lpe-ct/tls/privkey.pem
LPE_CT_SUBMISSION_TLS_CERT_PATH=/etc/lpe-ct/tls/fullchain.pem
LPE_CT_SUBMISSION_TLS_KEY_PATH=/etc/lpe-ct/tls/privkey.pem
LPE_CT_IMAPS_TLS_CERT_PATH=/etc/lpe-ct/tls/fullchain.pem
LPE_CT_IMAPS_TLS_KEY_PATH=/etc/lpe-ct/tls/privkey.pem
```

`nginx` uses the public pair for `443`. The Rust `LPE-CT` service uses the
submission pair for `465` and the IMAPS pair for `993`.

For `993`, `LPE-CT` terminates client `TLS` and then proxies the clear internal
IMAP stream to `LPE_CT_IMAPS_UPSTREAM_ADDRESS`. The default
`127.0.0.1:1143` is valid only when the core `LPE` IMAP listener is co-located
on the same host. In the normal split `DMZ` / `LAN` topology, set this to the
private LAN address and port of the core `LPE` IMAP listener, for example
`192.168.1.25:1143`, and allow that flow from `LPE-CT` to `LPE`.

On the core `LPE` server, configure the matching private listener in
`/etc/lpe/lpe.env`:

```bash
LPE_IMAP_BIND_ADDRESS=192.168.1.25:1143
LPE_IMAP_BIND_HOST=192.168.1.25
LPE_IMAP_BIND_PORT=1143
```

Then restart `LPE` and validate the listener locally:

```bash
systemctl restart lpe
cd /opt/lpe/src/installation/debian-trixie
sudo ./test-lpe-imap-listener.sh
```

For an authenticated protocol check, pass a real mailbox address and password.
The script then verifies `CAPABILITY`, literal-form `LOGIN`, and `SELECT INBOX`
against the core listener:

```bash
sudo LPE_IMAP_TEST_EMAIL=user@example.com \
  LPE_IMAP_TEST_PASSWORD='mailbox-password' \
  ./test-lpe-imap-listener.sh
```

From the `LPE-CT` server, `test-lpe-ct-edge-ports.sh` verifies both public
`993` TLS and reachability to `LPE_CT_IMAPS_UPSTREAM_ADDRESS`. For an
Outlook-equivalent check, pass the same mailbox credentials used with the core
IMAP test as `LPE_CT_OUTLOOK_TEST_EMAIL` and `LPE_CT_OUTLOOK_TEST_PASSWORD`.
The script then verifies trusted public TLS for the client hostname, Outlook
autodiscover IMAP/SMTP publication, public IMAPS login, Outlook-style folder
discovery, `STATUS INBOX`, and `SELECT INBOX` through the `LPE-CT` proxy. It
also authenticates to public `465` with `AUTH LOGIN` and verifies `MAIL FROM` /
`RCPT TO` acceptance without sending a message. It also probes SOAP
Autodiscover to ensure the default Outlook `IMAP` setup path is not given a
partial Exchange profile. The `outlook` scope fails when these credentials are
missing because TLS-only checks are not enough to prove Outlook can create the
profile:

```bash
sudo HOST=mail.example.com \
  LPE_CT_EDGE_TEST_SCOPE=outlook \
  LPE_CT_PUBLICATION_TEST_HOST=mail.example.com \
  LPE_CT_OUTLOOK_TEST_EMAIL=user@example.com \
  LPE_CT_OUTLOOK_TEST_PASSWORD='mailbox-password' \
  ./test-lpe-ct-edge-ports.sh
```

If a deployment needs different identities for autodiscover, IMAPS, or
submission, the protocol-specific overrides remain available:
`LPE_CT_AUTODISCOVER_TEST_EMAIL`, `LPE_CT_IMAPS_TEST_EMAIL`,
`LPE_CT_IMAPS_TEST_PASSWORD`, `LPE_CT_SUBMISSION_TEST_EMAIL`, and
`LPE_CT_SUBMISSION_TEST_PASSWORD`.

If the upstream probe fails, check that `LPE_IMAP_BIND_ADDRESS` is not
loopback-only and that the LAN firewall allows `LPE-CT` to connect to the core
`LPE` address on `1143`. If the authenticated public IMAPS probe fails before
`lpe.service` receives anything, inspect `journalctl -u lpe-ct.service` because
the failure is still on the TLS/proxy edge path. The full edge test uses
`LPE_CT_EDGE_TEST_SCOPE=all` by default; set the scope to `smtp`, `https`,
`submission`, `imaps`, `outlook`, or a comma-separated subset when isolating one
client path. The `outlook` scope intentionally skips public `25` and combines
the checks Outlook desktop needs for `IMAP` account setup. If autodiscover is
intentionally published on a different IMAP or SMTP hostname, set
`LPE_CT_EXPECTED_AUTODISCOVER_IMAP_HOST` or
`LPE_CT_EXPECTED_AUTODISCOVER_SMTP_HOST` for that run. The same scope fails if
autodiscover publishes Exchange-style `EXCH`, `EXPR`, `WEB`, or `mapiHttp`
blocks unless `LPE_CT_EXPECTED_OUTLOOK_EXCHANGE_AUTODISCOVER=true` is set,
because those blocks can make Outlook choose the unfinished Exchange route
instead of the working `IMAP` profile. It does not require `ActiveSync` or
`MAPI` OPTIONS checks; run `LPE_CT_EDGE_TEST_SCOPE=https` or `all` for those
publication checks.

The management UI URL must use `https://`. The generated `nginx` site redirects
plain `HTTP` received on port `80` to the configured
`LPE_CT_NGINX_LISTEN_PORT`, and also converts nginx's plain-HTTP-on-TLS listener
condition into a permanent redirect instead of exposing the default
`400 Bad Request: The plain HTTP request was sent to HTTPS port` page. If a
browser redirects and then reports `connection refused`, verify that nginx is
actually listening on `LPE_CT_NGINX_LISTEN_PORT`, default `443`, and that the
local firewall allows that port. `LPE_CT_NGINX_LISTEN_PORT=80` is invalid for
`LPE-CT` because port `80` is reserved for the redirect-only cleartext listener.

### Initial preparation on a bare Debian server

Update the APT index and install the minimal tools required to fetch the repository:

```bash
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl git
```

Then clone the repository with a sparse Git checkout limited to the Debian scripts:

```bash
git clone --filter=blob:none --no-checkout https://github.com/dducret/LPE ~/LPE-bootstrap
cd ~/LPE-bootstrap
git sparse-checkout init --cone
git sparse-checkout set installation/debian-trixie
git checkout main
cd installation/debian-trixie
chmod +x *.sh
```

This initial clone into `~/LPE-bootstrap` is only a bootstrap step for documentation and operations and is limited to the Debian scripts. The final product does not run from that directory.

The install and update scripts automatically register `/opt/lpe/src` as a Git `safe.directory` when they run as root.

Files:

- `install-lpe.sh` installs prerequisites, clones the repository, builds `lpe-cli`, and installs the systemd service
- `install-lpe.sh` is now interactive when run in a terminal for first-install values such as the public hostname, PostgreSQL settings, bootstrap administrator, integration secret, install directory, and service actions
- `install-lpe.sh` also provisions the pinned `Magika` CLI binary with checksum verification into `/opt/lpe/bin/magika`
- `install-lpe.sh` writes installer layout settings to `/etc/lpe/install.env`, writes runtime settings to `/etc/lpe/lpe.env`, and starts `lpe.service` only when the selected service action enables it
- `install-lpe.sh` now defaults to running `init-schema.sh` on first install before starting services, so a fresh node comes up against the initialized current schema
- `install-lpe.sh` now verifies that `LPE_DB_PASSWORD`, `DATABASE_URL`, `LPE_BOOTSTRAP_ADMIN_EMAIL`, `LPE_BOOTSTRAP_ADMIN_PASSWORD`, and `LPE_INTEGRATION_SHARED_SECRET` were actually persisted to `/etc/lpe/lpe.env` before it continues
- `install-lpe.sh` writes `DATABASE_URL` to `/etc/lpe/lpe.env`; when an older env file still lacks it, maintenance scripts derive it from `LPE_DB_HOST`, `LPE_DB_PORT`, `LPE_DB_NAME`, `LPE_DB_USER`, and `LPE_DB_PASSWORD`
- `install-lpe.sh` also installs `nodejs`, `npm`, and `nginx`, builds `web/admin` and `web/client`, deploys the static UIs, and enables the `nginx` site
- `update-lpe.sh` remains non-interactive, reuses `/etc/lpe/install.env` and `/etc/lpe/lpe.env`, applies non-destructive schema migrations from `crates/lpe-storage/sql/migrations`, rebuilds `lpe-cli`, and restarts the service
- `update-lpe.sh` also re-provisions the same pinned `Magika` version so content validation stays deterministic
- `update-lpe.sh` also rebuilds `web/admin` and `web/client`, redeploys static assets, and reloads `nginx`
- `bootstrap-postgresql.sh` creates a PostgreSQL role and database
- `bootstrap-postgresql.sh` also installs the PostgreSQL server if needed and starts it
- `create-lpe-database.sql` provides a SQL-native bootstrap alternative for creating the PostgreSQL role and database
- `crates/lpe-storage/sql/schema.sql` provides the canonical full schema for fresh databases
- `crates/lpe-storage/sql/migrations` contains non-destructive update migrations for already-initialized databases
- the installation scripts use the system `rustup` binary and initialize the `stable` toolchain before building
- `init-schema.sh` drops and recreates the PostgreSQL `public` schema, then applies the canonical schema; use it only for fresh installs or intentional resets
- `migrate-schema.sh` checks the installed schema version, applies pending non-destructive migrations, and refuses to reset or silently create a missing schema
- `check-lpe.sh` verifies the installation, PostgreSQL, the service, and the HTTP endpoints
- `check-lpe-ready.sh` returns success only when the local `LPE` node is ready for traffic
- `lpe-ha-set-role.sh` writes the local HA role (`active`, `standby`, `drain`, `maintenance`)
- `test-ha-core-active-passive.sh` validates local core HA role gating and readiness transitions
- `keepalived-lpe.conf.example` shows the minimal integration with a core-side VIP
- `lpe.service` describes the initial systemd service
- `lpe.nginx.conf` is the template used to generate the administration `nginx` site
- `lpe.env.example` provides a base configuration
- `high-availability.md` documents the first active/passive runbook for `LPE` and `LPE-CT` on `Debian Trixie`

Recommended order:

1. run `bootstrap-postgresql.sh`
2. run `install-lpe.sh`
3. verify the service with `systemctl status lpe.service`
4. open `http://server-address/` to reach the administration console through `nginx`
5. open `http://server-address/mail/` to reach the web client

The web client requires user authentication. First create an account and its password from the administration domain page, then sign in to `/mail/` with the full email address and that password.

The administration console stores its accounts, account passwords, mailboxes, `PST` import/export requests, domains, aliases, settings, delegated administrators, anti-spam objects, and audit events in `PostgreSQL`. Initialize a fresh database with `init-schema.sh`. Routine updates use `migrate-schema.sh` through `update-lpe.sh` and preserve existing data.

`PST` imports can be uploaded from the browser. The service validates each incoming file with Google `Magika` before storing it in `LPE_PST_IMPORT_DIR`, defaulting to `/var/lib/lpe/imports`, and then creates the `PST` import request with the resulting server path. The maximum accepted API upload size is configured through `LPE_PST_UPLOAD_MAX_BYTES`, defaulting to `21474836480` bytes. The `nginx` reverse proxy is aligned through `LPE_NGINX_CLIENT_MAX_BODY_SIZE`, defaulting to `20g`. The binary path is configured through `LPE_MAGIKA_BIN`, defaulting to `/opt/lpe/bin/magika`, and the minimum confidence threshold through `LPE_MAGIKA_MIN_SCORE`.

When no administrator exists yet, `LPE` now creates a bootstrap administrator automatically at startup from `LPE_BOOTSTRAP_ADMIN_EMAIL`, `LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME`, and `LPE_BOOTSTRAP_ADMIN_PASSWORD`. First startup now requires a real bootstrap email address and a strong password in `/etc/lpe/lpe.env`; there is no fallback runtime administrator or published default secret.

The back office now also supports a first federated `OIDC` login for administrators. Configuration is done from the console `Security` page and requires:

- explicit `OIDC` login enablement
- provider label
- `issuer URL`
- authorization endpoint
- token endpoint
- `userinfo` endpoint
- `client ID`
- `client secret`
- scopes
- claim names for subject, email, and display name

Federated login does not remove local password login by default. Local password login remains recommended for bootstrap, recovery, and break-glass access. The current model still does not allow automatic administrator provisioning from the `IdP`: the administrator must first exist in `LPE`, then email auto-link can be enabled to allow the first binding for an already existing administrator with the same address. Later federated logins then reuse the persisted identity mapping.

Complete example:

```bash
cd ~/LPE-bootstrap/installation/debian-trixie
./bootstrap-postgresql.sh
./install-lpe.sh
systemctl status lpe.service
./check-lpe.sh
```

### Interactive first install

`install-lpe.sh` and `LPE-CT/installation/debian-trixie/install-lpe-ct.sh` now prompt section by section when they detect an interactive terminal.

Prompt behavior:

- prompts show defaults in the form `Question label (default value):`
- pressing `Enter` accepts the displayed default
- required values without a safe default are shown as required and are re-prompted until valid
- leading and trailing whitespace is trimmed
- yes/no prompts accept `y`, `yes`, `n`, `no`, and `Enter` for the default
- secrets are not echoed; when a secret already exists, pressing `Enter` keeps the current value

The `LPE` installer prompts for:

- installation directory, fixed to `/opt/lpe`
- public hostname, no default
- server name, defaulting to the selected public hostname
- local service host, default `127.0.0.1`
- local service port, default `8080`
- internal IMAP host for `LPE-CT`, default `127.0.0.1`; in split `DMZ` / `LAN`
  topologies this must be the core `LPE` private LAN address
- internal IMAP port for `LPE-CT`, default `1143`
- HTTPS port, default `80`
- PostgreSQL host, default `localhost`
- PostgreSQL port, default `5432`
- PostgreSQL database name, default `lpe`
- PostgreSQL username, default `lpe`
- PostgreSQL password, no default
- `LPE-CT` API base URL, no default
- integration shared secret, no default and at least `32` characters
- bootstrap administrator email, no default
- bootstrap administrator display name, default `Bootstrap Administrator`
- bootstrap administrator password, no default and at least `12` characters
- whether to enable and start services now, default `yes`
- whether to run schema setup or migrations now, default `yes` on first install and `no` on later reruns

The `LPE-CT` installer prompts for:

- installation directory, fixed to `/opt/lpe-ct`
- public hostname, no default
- server name, defaulting to the selected public hostname
- local management host, default `127.0.0.1`
- local management port, default `8380`
- SMTP ingress host, default `0.0.0.0`
- SMTP ingress port, default `25`
- HTTPS port, default `443`; do not set this to `80`, which is reserved for
  the HTTP-to-HTTPS redirect
- public TLS certificate path, default `/etc/lpe-ct/tls/fullchain.pem`
- public TLS private key path, default `/etc/lpe-ct/tls/privkey.pem`
- IMAPS bind address, default `0.0.0.0:993`
- internal `LPE` IMAP upstream address, default `127.0.0.1:1143`
- SMTP submission bind address, default `0.0.0.0:465`
- internal `LPE` delivery URL, default `http://127.0.0.1:8080`
- integration shared secret, no default and at least `32` characters
- quarantine root path, default `/var/spool/lpe-ct`
- local `PostgreSQL` host, default `127.0.0.1`
- local `PostgreSQL` port, default `5432`
- local `PostgreSQL` database name, default `lpe_ct`
- local `PostgreSQL` username, default `lpe_ct`
- local `PostgreSQL` password, no default
- bootstrap administrator email, no default
- bootstrap administrator password, no default
- whether to enable and start services now, default `yes`

Selected runtime values are written back to `/etc/lpe/lpe.env` or `/etc/lpe-ct/lpe-ct.env`. Selected install-layout values such as `INSTALL_ROOT`, `SRC_DIR`, `BIN_DIR`, `WEB_ROOT`, and service directories are written to `/etc/lpe/install.env` or `/etc/lpe-ct/install.env` so later `update` runs stay non-interactive and reuse the installed paths.

The authenticated submission listener is configured by the installer on implicit `TLS` port `465`. It uses:

- `LPE_CT_SUBMISSION_BIND_ADDRESS`, typically `0.0.0.0:465`
- `LPE_CT_SUBMISSION_TLS_CERT_PATH`
- `LPE_CT_SUBMISSION_TLS_KEY_PATH`
- optionally `LPE_CT_SUBMISSION_MAX_MESSAGE_SIZE_MB`

### Unattended installs

Both first-install scripts remain unattended-friendly. In non-interactive mode, by `--non-interactive` or when no interactive `TTY` is available, they:

- use explicit environment variables first
- fall back to documented defaults only for safe values
- fail clearly for required values that do not have a safe default

Typical unattended `LPE` environment variables:

- `LPE_PUBLIC_HOSTNAME`
- `LPE_SERVER_NAME`
- `LPE_LOCAL_BIND_HOST`
- `LPE_LOCAL_BIND_PORT`
- `LPE_NGINX_LISTEN_PORT`
- `LPE_DB_HOST`
- `LPE_DB_PORT`
- `LPE_DB_NAME`
- `LPE_DB_USER`
- `LPE_DB_PASSWORD`
- `LPE_CT_API_BASE_URL`
- `LPE_INTEGRATION_SHARED_SECRET`
- `LPE_BOOTSTRAP_ADMIN_EMAIL`
- `LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME`
- `LPE_BOOTSTRAP_ADMIN_PASSWORD`
- `LPE_ENABLE_SERVICES`
- `LPE_RUN_MIGRATIONS`

The effective default PostgreSQL connection string shape is:

```bash
postgres://<LPE_DB_USER>:<LPE_DB_PASSWORD>@<LPE_DB_HOST>:<LPE_DB_PORT>/<LPE_DB_NAME>
```

With the shipped non-secret defaults, that becomes:

```bash
postgres://lpe:<password>@localhost:5432/lpe
```

Typical unattended `LPE-CT` environment variables:

- `LPE_CT_PUBLIC_HOSTNAME`
- `LPE_CT_SERVER_NAME`
- `LPE_CT_BIND_HOST`
- `LPE_CT_BIND_PORT`
- `LPE_CT_SMTP_HOST`
- `LPE_CT_SMTP_PORT`
- `LPE_CT_NGINX_LISTEN_PORT`
- `LPE_CT_PUBLIC_TLS_CERT_PATH`
- `LPE_CT_PUBLIC_TLS_KEY_PATH`
- `LPE_CT_IMAPS_BIND_ADDRESS`
- `LPE_CT_IMAPS_UPSTREAM_ADDRESS`
- `LPE_CT_IMAPS_TLS_CERT_PATH`
- `LPE_CT_IMAPS_TLS_KEY_PATH`
- `LPE_CT_SUBMISSION_BIND_ADDRESS`
- `LPE_CT_SUBMISSION_TLS_CERT_PATH`
- `LPE_CT_SUBMISSION_TLS_KEY_PATH`
- `LPE_CT_CORE_DELIVERY_BASE_URL`
- `LPE_INTEGRATION_SHARED_SECRET`
- `SPOOL_DIR`
- `LPE_CT_LOCAL_DB_HOST`
- `LPE_CT_LOCAL_DB_PORT`
- `LPE_CT_LOCAL_DB_NAME`
- `LPE_CT_LOCAL_DB_USER`
- `LPE_CT_LOCAL_DB_PASSWORD`
- `LPE_CT_BOOTSTRAP_ADMIN_EMAIL`
- `LPE_CT_BOOTSTRAP_ADMIN_PASSWORD`
- `LPE_CT_ENABLE_SERVICES`

Example unattended `LPE` first install:

```bash
LPE_PUBLIC_HOSTNAME=mail.example.com \
LPE_SERVER_NAME=mail.example.com \
LPE_LOCAL_BIND_HOST=10.20.0.40 \
LPE_LOCAL_BIND_PORT=8080 \
LPE_IMAP_BIND_ADDRESS=10.20.0.40:1143 \
LPE_IMAP_BIND_HOST=10.20.0.40 \
LPE_IMAP_BIND_PORT=1143 \
LPE_DB_HOST=127.0.0.1 \
LPE_DB_PORT=5432 \
LPE_DB_NAME=lpe \
LPE_DB_USER=lpe \
LPE_DB_PASSWORD='replace-with-strong-password' \
LPE_CT_API_BASE_URL=http://10.20.10.40:8380 \
LPE_INTEGRATION_SHARED_SECRET='replace-with-a-secret-of-at-least-32-characters' \
LPE_BOOTSTRAP_ADMIN_EMAIL=admin@example.com \
LPE_BOOTSTRAP_ADMIN_PASSWORD='replace-with-strong-password' \
LPE_ENABLE_SERVICES=yes \
LPE_RUN_MIGRATIONS=yes \
./install-lpe.sh --non-interactive
```

Example unattended `LPE-CT` first install:

```bash
LPE_CT_PUBLIC_HOSTNAME=mx.example.com \
LPE_CT_SERVER_NAME=mx.example.com \
LPE_CT_SMTP_HOST=0.0.0.0 \
LPE_CT_SMTP_PORT=25 \
LPE_CT_NGINX_LISTEN_PORT=443 \
LPE_CT_PUBLIC_TLS_CERT_PATH=/etc/lpe-ct/tls/fullchain.pem \
LPE_CT_PUBLIC_TLS_KEY_PATH=/etc/lpe-ct/tls/privkey.pem \
LPE_CT_IMAPS_BIND_ADDRESS=0.0.0.0:993 \
LPE_CT_IMAPS_UPSTREAM_ADDRESS=10.20.0.40:1143 \
LPE_CT_IMAPS_TLS_CERT_PATH=/etc/lpe-ct/tls/fullchain.pem \
LPE_CT_IMAPS_TLS_KEY_PATH=/etc/lpe-ct/tls/privkey.pem \
LPE_CT_SUBMISSION_BIND_ADDRESS=0.0.0.0:465 \
LPE_CT_SUBMISSION_TLS_CERT_PATH=/etc/lpe-ct/tls/fullchain.pem \
LPE_CT_SUBMISSION_TLS_KEY_PATH=/etc/lpe-ct/tls/privkey.pem \
LPE_CT_CORE_DELIVERY_BASE_URL=http://10.20.0.40:8080 \
LPE_INTEGRATION_SHARED_SECRET='replace-with-a-secret-of-at-least-32-characters' \
LPE_CT_LOCAL_DB_HOST=127.0.0.1 \
LPE_CT_LOCAL_DB_PORT=5432 \
LPE_CT_LOCAL_DB_NAME=lpe_ct \
LPE_CT_LOCAL_DB_USER=lpe_ct \
LPE_CT_LOCAL_DB_PASSWORD='replace-with-strong-password' \
LPE_CT_BOOTSTRAP_ADMIN_EMAIL=admin@example.com \
LPE_CT_BOOTSTRAP_ADMIN_PASSWORD='replace-with-strong-password' \
LPE_CT_ENABLE_SERVICES=yes \
./install-lpe-ct.sh --non-interactive
```

Optional upstream smart-host targets are intentionally omitted from the service
environment. Configure them only in the `LPE-CT` Web GUI under `System Setup ->
Mail relay -> General Settings`, where they are persisted in the private
`LPE-CT` PostgreSQL dashboard state. By default `LPE-CT` is the outbound gateway
and delivers through recipient-domain `MX` routing.
Port `2525` is not the canonical `LPE <-> LPE-CT` bridge. Final delivery into
`LPE` uses
`${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/inbound-deliveries`; outbound
handoff from `LPE` uses
`${LPE_CT_API_BASE_URL}/api/v1/integration/outbound-messages`.

By default:

- `lpe.service` listens on `127.0.0.1:8080`
- `nginx` exposes the administration console on port `80`
- `nginx` exposes the web client on `/mail/`
- `nginx` reverse-proxies `/api/` to the local Rust service
- `nginx` also publishes `/Microsoft-Server-ActiveSync`
- `nginx` also publishes `/autodiscover/autodiscover.xml` and `/Autodiscover/Autodiscover.xml`
- `nginx` also publishes `/autoconfig/mail/config-v1.1.xml` and `/.well-known/autoconfig/mail/config-v1.1.xml`

In a split `DMZ` / `LAN` topology, the default core HTTP listener is not enough:
`127.0.0.1:8080` is reachable only from the core host itself. Set
`LPE_LOCAL_BIND_HOST` / `LPE_LOCAL_BIND_PORT`, or `LPE_BIND_ADDRESS`, on the
core `LPE` node to its private LAN address, for example `10.20.0.40:8080`, then
set `LPE_CT_CORE_DELIVERY_BASE_URL=http://10.20.0.40:8080` on the `LPE-CT`
node. After changing the core bind address, run `update-lpe.sh`, restart
`lpe.service`, and verify from the `LPE-CT` node with
`curl http://10.20.0.40:8080/health/live` before rerunning the edge tests.

The core `LPE` nginx template also emits the non-HSTS baseline browser security
headers for its local administration and webmail publication. HSTS remains the
responsibility of the public `HTTPS` edge, normally `LPE-CT`.

On the public edge, `LPE-CT` must publish HTTPS on `443` with
`LPE_CT_PUBLIC_TLS_CERT_PATH` and `LPE_CT_PUBLIC_TLS_KEY_PATH`. The same
certificate files may be reused for `465` submission through
`LPE_CT_SUBMISSION_TLS_CERT_PATH` / `LPE_CT_SUBMISSION_TLS_KEY_PATH`, and for
`993` IMAPS through `LPE_CT_IMAPS_TLS_CERT_PATH` /
`LPE_CT_IMAPS_TLS_KEY_PATH`. The certificate must cover the public `LPE-CT`
hostname used by clients.

`LPE-CT` must publish the mailbox login and `JMAP` HTTPS/WSS paths and proxy
them to the core `LPE` service: `/api/mail/auth/login`,
`/api/jmap/session`, `/api/jmap/api`,
`/api/jmap/upload/{accountId}`, `/api/jmap/download/{accountId}/{blobId}/{name}`,
and `/api/jmap/ws`.

`LPE-CT` must also publish the public client configuration, `ActiveSync`, EWS,
and guarded MAPI paths: `/Microsoft-Server-ActiveSync`, `/mapi/`,
`/EWS/Exchange.asmx`, `/ews/exchange.asmx`, `/autodiscover`,
`/autodiscover/`, `/Autodiscover`, `/Autodiscover/`, `/autoconfig/`,
`/.well-known/autoconfig/`, and `/.well-known/jmap`. A healthy public
publication returns an Outlook autodiscover XML response containing `IMAP`,
an Autodiscover v2 JSON response for single-protocol endpoint probes, an
opt-in `WEB` EWS discovery block when
`LPE_AUTOCONFIG_EWS_ENABLED` is enabled, an opt-in `mapiHttp` block when
`LPE_AUTOCONFIG_MAPI_ENABLED` is enabled, or legacy `EXCH` / `EXPR` provider
sections only when `LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED` is
also enabled with an explicitly published EWS or MAPI surface. `OPTIONS
/Microsoft-Server-ActiveSync` returns the `ms-asprotocolversions` and
`ms-asprotocolcommands` headers. `OPTIONS /mapi/emsmdb` returns
`x-lpe-mapi-status: transport-session-ready`.

For public client auto-configuration, the exposed front end must remain `LPE-CT` or an equivalent HTTPS publication layer. In v1:

- `Thunderbird` receives an `IMAP` profile
- Outlook for Windows desktop receives an `IMAP` profile by default; `IMAP` remains a supported mailbox-access communication protocol, while `0.1.3` deployments may explicitly enable EWS autodiscovery for the implemented Exchange-style compatibility surface
- `ActiveSync` remains exposed for mobile/native clients that actually support `Exchange ActiveSync`
- `EWS` remains opt-in through `LPE_AUTOCONFIG_EWS_ENABLED` and must not be treated as `MAPI`, `RPC`, or client `SMTP`
- `MAPI over HTTP` routes are guarded implementation groundwork; the public edge publishes `/mapi/` so Outlook can reach the authenticated endpoints, but autodiscover publishes `mapiHttp` only when `LPE_AUTOCONFIG_MAPI_ENABLED` is explicitly enabled for interoperability testing, SOAP Exchange `GetUserSettings` only when `LPE_AUTOCONFIG_SOAP_EXCHANGE_AUTODISCOVER_ENABLED` is also enabled, and legacy `EXCH` / `EXPR` provider sections only when `LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED` is also enabled with an explicitly published EWS or MAPI surface
- Microsoft Remote Connectivity Analyzer Outlook Connectivity expects a top-level `EXCH` provider section; for the EWS compatibility path, set both `LPE_AUTOCONFIG_EWS_ENABLED=true` and `LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED=true`
- no client `SMTP` endpoint should be advertised unless the authenticated `LPE-CT` submission listener is configured, exposed on `465`, and covered by the public certificate
- the internal `LPE -> LPE-CT` relay must never be advertised as a client-submission endpoint

The `LPE_PUBLIC_SCHEME`, `LPE_PUBLIC_HOSTNAME`, `LPE_AUTOCONFIG_IMAP_HOST`, `LPE_AUTOCONFIG_IMAP_PORT`, `LPE_AUTOCONFIG_SMTP_HOST`, `LPE_AUTOCONFIG_SMTP_PORT`, `LPE_AUTOCONFIG_SMTP_SOCKET_TYPE`, `LPE_AUTOCONFIG_EWS_ENABLED`, `LPE_AUTOCONFIG_EWS_URL`, `LPE_AUTOCONFIG_MAPI_ENABLED`, `LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED`, `LPE_AUTOCONFIG_SOAP_EXCHANGE_AUTODISCOVER_ENABLED`, `LPE_AUTOCONFIG_MAPI_EMSMDB_URL`, and `LPE_AUTOCONFIG_MAPI_NSPI_URL` variables let you align the published HTTP/XML settings with the real public hostname. The detailed behavior is documented in `docs/architecture/client-autoconfiguration.md`.

If `LPE_BIND_ADDRESS` or `LPE_SERVER_NAME` changes in `/etc/lpe/lpe.env`, run `update-lpe.sh` again to regenerate the `nginx` configuration. If `LPE_IMAP_BIND_ADDRESS` changes, restart `lpe.service` and rerun `test-lpe-imap-listener.sh` on the core server, then rerun `test-lpe-ct-edge-ports.sh` on the `LPE-CT` server.

For later updates:

1. push the desired commit to `https://github.com/dducret/LPE`
2. run `update-lpe.sh`

`update-lpe.sh` is no longer destructive by default. It applies pending SQL migrations and exits with an error if the database is uninitialized or if the checked-out code expects a schema version for which no migration has been provided. For an intentional destructive reset, run `init-schema.sh` explicitly.

`LPE-CT/installation/debian-trixie/update-lpe-ct.sh` is not destructive by default. It rebuilds and redeploys the service while preserving the full spool, retained history, the private local PostgreSQL state, and the legacy `state.json` bootstrap/export file unless `LPE_CT_RESET_STATE_ON_UPDATE=true` is set explicitly for a disposable environment.

When `LPE_CT_LOCAL_DB_ENABLED=true`, `LPE_CT_LOCAL_DB_URL` and the private PostgreSQL store are required at startup because the LPE-CT management dashboard state is persisted there. Queue payload custody remains in `/var/spool/lpe-ct`, while `state.json` is only a legacy bootstrap/export file.

If you want to fetch the latest scripts first before an update:

```bash
cd /opt/lpe/src
git pull --ff-only origin main
cd installation/debian-trixie
./update-lpe.sh
./check-lpe.sh
```

To validate the installation:

1. run `check-lpe.sh`

### First Implementable HA Step

This first HA step remains optional and does not change the default single-node deployment.

For an `active/passive` `LPE` core pair:

1. set `LPE_HA_ROLE_FILE=/var/lib/lpe/ha-role` in `/etc/lpe/lpe.env`
2. initialize the active node with `installation/debian-trixie/lpe-ha-set-role.sh active`
3. initialize the passive node with `installation/debian-trixie/lpe-ha-set-role.sh standby`
4. use `check-lpe-ready.sh` as the readiness probe for `keepalived` or an equivalent front end
5. point `DATABASE_URL` to the active PostgreSQL writer and `LPE_CT_API_BASE_URL` to the `DMZ` VIP

During failover:

1. promote PostgreSQL outside the application
2. move the core VIP
3. switch the new master node to `active`
4. switch the former master to `standby` or `maintenance`
5. verify `curl http://127.0.0.1:8080/health/ready`

For an `active/passive` `LPE-CT` pair:

1. set `LPE_CT_HA_ROLE_FILE=/var/lib/lpe-ct/ha-role` in `/etc/lpe-ct/lpe-ct.env`
2. initialize the active node with `LPE-CT/installation/debian-trixie/lpe-ct-ha-set-role.sh active`
3. initialize the passive node with `LPE-CT/installation/debian-trixie/lpe-ct-ha-set-role.sh standby`
4. use `check-lpe-ct-ready.sh` as the readiness probe for `keepalived` or an equivalent front end
5. validate the node locally with `test-ha-lpe-ct-active-passive.sh`

When a failed `LPE-CT` node returns, inventory and requeue its local spool with `lpe-ct-spool-recover.sh` before returning it to service.


