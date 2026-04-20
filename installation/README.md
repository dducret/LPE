# Installation

These installation and update instructions are aligned with repository release `0.1.20`.

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

For a separate sorting server in the `DMZ`, use `LPE-CT/installation/debian-trixie` instead. That subdirectory installs a distinct component into `/opt/lpe-ct` with its own management UI, without exposing the core back office on the DMZ server, and also provisions a pinned `Magika` CLI binary in `/opt/lpe-ct/bin/magika` for inbound SMTP validation.

The `LPE-CT` scripts also install an SMTP listener, a local spool in `/var/spool/lpe-ct`, and three test suites:

For the first `active/passive` `DMZ` deployment step, `LPE-CT/installation/debian-trixie` also provides `check-lpe-ct-ready.sh`, `lpe-ct-ha-set-role.sh`, and `keepalived-lpe-ct.conf.example`.

The functional `LPE` / `LPE-CT` integration also requires aligned `LPE_CT_CORE_DELIVERY_BASE_URL`, `LPE_CT_API_BASE_URL`, and `LPE_INTEGRATION_SHARED_SECRET` values across the two nodes. `LPE_INTEGRATION_SHARED_SECRET` is now mandatory on both sides at startup, must stay out of public interfaces, and must be set to a strong non-trivial value of at least `32` characters. The contract is documented in `docs/architecture/lpe-ct-integration.md`.

- `test-local-lpe-ct.sh` from the `LPE-CT` server
- `test-from-lpe.sh` from the LAN or core server
- `test-from-internet.sh` from an external machine

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
- `install-lpe.sh` also provisions the pinned `Magika` CLI binary with checksum verification into `/opt/lpe/bin/magika`
- `install-lpe.sh` also starts `lpe.service` at the end of the installation
- `install-lpe.sh` also installs `nodejs`, `npm`, and `nginx`, builds `web/admin` and `web/client`, deploys the static UIs, and enables the `nginx` site
- `update-lpe.sh` updates the repository, applies SQL migrations, rebuilds `lpe-cli`, and restarts the service
- `update-lpe.sh` also re-provisions the same pinned `Magika` version so content validation stays deterministic
- `update-lpe.sh` also rebuilds `web/admin` and `web/client`, redeploys static assets, and reloads `nginx`
- `bootstrap-postgresql.sh` creates a PostgreSQL role and database
- `bootstrap-postgresql.sh` also installs the PostgreSQL server if needed and starts it
- the installation scripts use the system `rustup` binary and initialize the `stable` toolchain before building
- `run-migrations.sh` applies the project's PostgreSQL SQL migrations
- `run-migrations.sh` also applies the persistent schema for the administration console
- `check-lpe.sh` verifies the installation, PostgreSQL, the service, and the HTTP endpoints
- `check-lpe-ready.sh` returns success only when the local `LPE` node is ready for traffic
- `lpe-ha-set-role.sh` writes the local HA role (`active`, `standby`, `drain`, `maintenance`)
- `keepalived-lpe.conf.example` shows the minimal integration with a core-side VIP
- `lpe.service` describes the initial systemd service
- `lpe.nginx.conf` is the template used to generate the administration `nginx` site
- `lpe.env.example` provides a base configuration
- `high-availability.md` documents the first active/passive runbook for `LPE` and `LPE-CT` on `Debian Trixie`

Recommended order:

1. run `bootstrap-postgresql.sh`
2. run `install-lpe.sh`
3. adjust `/etc/lpe/lpe.env`
4. run `run-migrations.sh`
5. verify the service with `systemctl status lpe.service`
6. open `http://server-address/` to reach the administration console through `nginx`
7. open `http://server-address/mail/` to reach the web client

The web client requires user authentication. First create an account and its password from the administration domain page, then sign in to `/mail/` with the full email address and that password.

The administration console now stores its accounts, account passwords, mailboxes, `PST` import/export requests, domains, aliases, settings, delegated administrators, anti-spam objects, and audit events in `PostgreSQL`. Running migrations is therefore mandatory after deployment or any schema update. `update-lpe.sh` applies them automatically after `git pull` so the API is not deployed ahead of the PostgreSQL schema.

`PST` imports can be uploaded from the browser. The service validates each incoming file with Google `Magika` before storing it in `LPE_PST_IMPORT_DIR`, defaulting to `/var/lib/lpe/imports`, and then creates the `PST` import request with the resulting server path. The maximum accepted API upload size is configured through `LPE_PST_UPLOAD_MAX_BYTES`, defaulting to `21474836480` bytes. The `nginx` reverse proxy is aligned through `LPE_NGINX_CLIENT_MAX_BODY_SIZE`, defaulting to `20g`. The binary path is configured through `LPE_MAGIKA_BIN`, defaulting to `/opt/lpe/bin/magika`, and the minimum confidence threshold through `LPE_MAGIKA_MIN_SCORE`.

When no administrator exists yet, `LPE` now creates a bootstrap administrator automatically at startup from `LPE_BOOTSTRAP_ADMIN_EMAIL`, `LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME`, and `LPE_BOOTSTRAP_ADMIN_PASSWORD`. The example configuration uses `admin@example.test` with `ChangeMeNow$` for the first operational sign-in. In production, that bootstrap secret must be changed immediately.

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
nano /etc/lpe/lpe.env
./run-migrations.sh
systemctl status lpe.service
./check-lpe.sh
```

By default:

- `lpe.service` listens on `127.0.0.1:8080`
- `nginx` exposes the administration console on port `80`
- `nginx` exposes the web client on `/mail/`
- `nginx` reverse-proxies `/api/` to the local Rust service
- `nginx` also publishes `/Microsoft-Server-ActiveSync`
- `nginx` also publishes `/autodiscover/autodiscover.xml` and `/Autodiscover/Autodiscover.xml`
- `nginx` also publishes `/autoconfig/mail/config-v1.1.xml` and `/.well-known/autoconfig/mail/config-v1.1.xml`

For public client auto-configuration, the exposed front end must remain `LPE-CT` or an equivalent HTTPS publication layer. In v1:

- `Thunderbird` receives an `IMAP` profile
- `Outlook` receives an `ActiveSync` profile
- no client `SMTP` endpoint is advertised by default because the repository does not yet expose authenticated `465/587` client submission
- the internal `LPE -> LPE-CT` relay must never be advertised as a client-submission endpoint

The `LPE_PUBLIC_SCHEME`, `LPE_PUBLIC_HOSTNAME`, `LPE_AUTOCONFIG_IMAP_HOST`, `LPE_AUTOCONFIG_IMAP_PORT`, `LPE_AUTOCONFIG_SMTP_HOST`, `LPE_AUTOCONFIG_SMTP_PORT`, `LPE_AUTOCONFIG_SMTP_SOCKET_TYPE`, and `LPE_AUTODISCOVER_ACTIVESYNC_URL` variables let you align the published HTTP/XML settings with the real public hostname. The detailed behavior is documented in `docs/architecture/client-autoconfiguration.md`.

If `LPE_BIND_ADDRESS` or `LPE_SERVER_NAME` changes in `/etc/lpe/lpe.env`, run `update-lpe.sh` again to regenerate the `nginx` configuration.

For later updates:

1. push the desired commit to `https://github.com/dducret/LPE`
2. run `update-lpe.sh`

`update-lpe.sh` runs `run-migrations.sh` automatically. This covers schema changes used by `/api/mail/workspace`, such as the `contacts` and `calendar_events` tables.

For development, functional reset, or MVP rebuild environments, `update-lpe.sh` also supports `LPE_RESET_DATABASE_ON_UPDATE=true`. In that mode, the script drops and recreates the PostgreSQL `public` schema before running migrations. This mode is destructive and must not be enabled on an instance that contains data you need to keep.

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


