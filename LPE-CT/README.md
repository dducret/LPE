# LPE-CT

## Francais

`LPE-CT` est le centre de tri `LPE` pour un serveur separe place en `DMZ`. Ce composant isole l'entree SMTP, les politiques de quarantaine, le relais sortant, la remise finale vers le coeur `LPE` en `LAN` via contrat interne explicite, et son interface de management dediee.

### Objectifs

- exposer en DMZ un noeud de tri distinct du serveur coeur
- limiter les flux vers le LAN aux relais et a la gestion explicitement autorises
- fournir une interface de management locale orientee exploitation
- conserver une installation `Debian Trixie` simple, reproductible et mise a jour depuis Git

### Contenu

- `src/` API de management Rust et listener SMTP pour le centre de tri
- `web/` interface statique de management servie par `nginx`
- `installation/debian-trixie/` scripts d'installation, mise a jour et verification
- `docs/architecture/center-de-tri.md` architecture et perimetre DMZ
- `docs/operations/mail-filtering.md` exploitation du pipeline SMTP, antispam et traçabilité

### Demarrage local

```powershell
cargo run --manifest-path LPE-CT/Cargo.toml
```

### Fonctions v1 deployables

- listener SMTP minimal `EHLO` / `MAIL FROM` / `RCPT TO` / `DATA` / `QUIT`
- spool local dans `incoming`, `deferred`, `quarantine`, `held` et `sent`
- handoff HTTP interne recu depuis `LPE` pour les messages sortants
- relais SMTP simple vers un upstream primaire puis secondaire pour la sortie
- classification sortante plus fine avec `deferred`, `bounced`, `failed` et detail `DSN`/technique
- regles locales de routage sortant
- throttling sortant par fenetre glissante locale
- remise finale des messages entrants vers `LPE` via `POST /internal/lpe-ct/inbound-deliveries`
- quarantaine de test via l'en-tete `X-LPE-CT-Quarantine: yes` ou un sujet contenant `[quarantine]`
- execution des controles `SPF`, `DKIM` et `DMARC` a l'entree
- decisions `reject` / `quarantine` / `defer` fondees sur des verdicts SPF/DKIM/DMARC structures, pas sur des comparaisons textuelles fragiles
- greylisting sur triplets `(IP source, MAIL FROM, premier RCPT TO)`
- lookups `DNSBL/RBL` configurables
- scoring antispam enrichi avec reputation locale simple et seuils de quarantaine/rejet par `(IP source, domaine emetteur)`
- trace detaillee des decisions de perimeterie dans les fichiers JSON du spool
- mode drainage qui accepte les messages et les place en `held`
- metriques de files exposees dans le dashboard de management

Par defaut, l'API ecoute sur `127.0.0.1:8380`, le SMTP ecoute sur `0.0.0.0:25`, l'etat est persiste dans `LPE_CT_STATE_FILE` et le spool dans `LPE_CT_SPOOL_DIR`.

Pour un test sans privilege port 25:

```powershell
$env:LPE_CT_SMTP_BIND_ADDRESS="127.0.0.1:2525"
cargo run --manifest-path LPE-CT/Cargo.toml
```

### Installation Debian Trixie

Depuis un bootstrap Git sparse ou depuis le checkout complet:

```bash
cd LPE-CT/installation/debian-trixie
chmod +x *.sh
./install-lpe-ct.sh
nano /etc/lpe-ct/lpe-ct.env
systemctl restart lpe-ct.service
./check-lpe-ct.sh
```

Configurer au minimum:

- `LPE_CT_SMTP_BIND_ADDRESS`, par defaut `0.0.0.0:25`
- `LPE_CT_CORE_DELIVERY_BASE_URL`, par exemple `http://10.20.0.20:8080`
- `LPE_CT_RELAY_PRIMARY`, par exemple `smtp://10.20.0.12:2525`
- `LPE_CT_RELAY_SECONDARY` si un second relais sortant existe
- `LPE_INTEGRATION_SHARED_SECRET`
- `LPE_CT_MUTUAL_TLS_REQUIRED=false` pour la v1 fonctionnelle actuelle
- `LPE_CT_GREYLISTING_ENABLED`, `LPE_CT_DNSBL_ZONES`, `LPE_CT_DEFER_ON_AUTH_TEMPFAIL`
- `LPE_CT_REPUTATION_QUARANTINE_THRESHOLD`, `LPE_CT_REPUTATION_REJECT_THRESHOLD`
- `LPE_CT_SPAM_QUARANTINE_THRESHOLD`, `LPE_CT_SPAM_REJECT_THRESHOLD`

Voir aussi `docs/operations/mail-filtering.md` pour le detail des scores, traces et politiques operatoires.

Fonctions encore hors scope dans ce lot:

- signature `DKIM` sortante
- `ARC`
- `MTA-STS`
- `TLS-RPT`
- reputation externe ou federée

### Jeux de tests

- `test-local-lpe-ct.sh` se lance sur le serveur `LPE-CT`
- `test-from-lpe.sh` se lance depuis une machine du LAN ou le serveur coeur `LPE`
- `test-from-internet.sh` se lance depuis une machine externe, avec verification optionnelle que le management n'est pas expose publiquement

Exemples:

```bash
./test-local-lpe-ct.sh
CT_HOST=mx1.example.test ./test-from-lpe.sh
CT_PUBLIC_HOST=mx1.example.test ./test-from-internet.sh
```

## English

`LPE-CT` is the `LPE` sorting center for a separate server placed in a `DMZ`. This component isolates SMTP ingress, quarantine policies, outbound relay, final delivery toward the core `LPE` services in the `LAN` through an explicit internal contract, and its dedicated management interface.

### Goals

- expose a DMZ edge node separate from the core server
- limit LAN flows to explicitly authorized relay and management traffic
- provide a local management interface focused on operations
- keep `Debian Trixie` installation simple, reproducible, and Git-driven

### Contents

- `src/` Rust management API and SMTP listener for the sorting center
- `web/` static management interface served by `nginx`
- `installation/debian-trixie/` install, update, and verification scripts
- `docs/architecture/center-de-tri.md` DMZ architecture and scope
- `docs/operations/mail-filtering.md` SMTP filtering, anti-spam, and traceability operations

### Local start

```powershell
cargo run --manifest-path LPE-CT/Cargo.toml
```

### Deployable v1 functions

- minimal SMTP listener for `EHLO` / `MAIL FROM` / `RCPT TO` / `DATA` / `QUIT`
- local spool in `incoming`, `deferred`, `quarantine`, `held`, and `sent`
- internal HTTP handoff received from `LPE` for outbound messages
- simple SMTP relay to a primary then secondary upstream for outbound transport
- richer outbound classification with `deferred`, `bounced`, `failed`, and structured `DSN`/technical detail
- local outbound routing rules
- local sliding-window outbound throttling
- final delivery of accepted inbound messages to `LPE` through `POST /internal/lpe-ct/inbound-deliveries`
- test quarantine through the `X-LPE-CT-Quarantine: yes` header or a subject containing `[quarantine]`
- executed inbound `SPF`, `DKIM`, and `DMARC` checks
- `reject` / `quarantine` / `defer` decisions driven by structured SPF/DKIM/DMARC outcomes rather than brittle string matching
- greylisting on `(source IP, MAIL FROM, first RCPT TO)` triplets
- configurable `DNSBL/RBL` lookups
- richer anti-spam scoring with simple local reputation and sender/IP quarantine or reject thresholds
- detailed perimeter-decision trace persisted in spool JSON files
- drain mode that accepts messages and places them in `held`
- queue metrics exposed in the management dashboard

By default, the API listens on `127.0.0.1:8380`, SMTP listens on `0.0.0.0:25`, state is persisted in `LPE_CT_STATE_FILE`, and the spool is stored in `LPE_CT_SPOOL_DIR`.

For a non-privileged port 25 test:

```powershell
$env:LPE_CT_SMTP_BIND_ADDRESS="127.0.0.1:2525"
cargo run --manifest-path LPE-CT/Cargo.toml
```

### Debian Trixie installation

From a sparse Git bootstrap or from the full checkout:

```bash
cd LPE-CT/installation/debian-trixie
chmod +x *.sh
./install-lpe-ct.sh
nano /etc/lpe-ct/lpe-ct.env
systemctl restart lpe-ct.service
./check-lpe-ct.sh
```

Configure at least:

- `LPE_CT_SMTP_BIND_ADDRESS`, default `0.0.0.0:25`
- `LPE_CT_CORE_DELIVERY_BASE_URL`, for example `http://10.20.0.20:8080`
- `LPE_CT_RELAY_PRIMARY`, for example `smtp://10.20.0.12:2525`
- `LPE_CT_RELAY_SECONDARY` if a second outbound relay exists
- `LPE_INTEGRATION_SHARED_SECRET`
- `LPE_CT_MUTUAL_TLS_REQUIRED=false` for the current functional v1
- `LPE_CT_GREYLISTING_ENABLED`, `LPE_CT_DNSBL_ZONES`, `LPE_CT_DEFER_ON_AUTH_TEMPFAIL`
- `LPE_CT_REPUTATION_QUARANTINE_THRESHOLD`, `LPE_CT_REPUTATION_REJECT_THRESHOLD`
- `LPE_CT_SPAM_QUARANTINE_THRESHOLD`, `LPE_CT_SPAM_REJECT_THRESHOLD`

See `docs/operations/mail-filtering.md` for the detailed score, trace, and policy workflow.

Still out of scope in this lot:

- outbound `DKIM` signing
- `ARC`
- `MTA-STS`
- `TLS-RPT`
- external or federated reputation feeds

### Test suites

- `test-local-lpe-ct.sh` runs on the `LPE-CT` server
- `test-from-lpe.sh` runs from a LAN host or the core `LPE` server
- `test-from-internet.sh` runs from an external machine, with optional verification that management is not publicly exposed

Examples:

```bash
./test-local-lpe-ct.sh
CT_HOST=mx1.example.test ./test-from-lpe.sh
CT_PUBLIC_HOST=mx1.example.test ./test-from-internet.sh
```
