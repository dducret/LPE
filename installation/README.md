# Installation

## Francais

### Debian Trixie

Le repertoire `debian-trixie` prepare une installation source de `LPE` depuis:

- `https://github.com/dducret/LPE`

Repertoire produit par defaut:

- `/opt/lpe`

Repertoire de bootstrap temporaire dans les exemples:

- `~/LPE-bootstrap`

Important:

- `~/LPE-bootstrap` sert uniquement a recuperer et lancer les scripts sur un serveur Debian nu
- l'installation reelle du produit est ensuite effectuee par `install-lpe.sh` dans `/opt/lpe`
- apres installation, le checkout de reference cote produit est `/opt/lpe/src`
- le bootstrap peut etre limite au sous-repertoire `installation/debian-trixie` via un checkout Git sparse
- le checkout produit dans `/opt/lpe/src` doit rester complet, car la compilation Rust a besoin du workspace

Hypothese d'exploitation:

- serveur Debian Trixie minimal
- pas d'interface graphique
- shell root ou compte avec `sudo`
- aucune dependance locale supposee au depart sauf l'acces reseau

Pour un serveur de tri separe en `DMZ`, utiliser plutot `LPE-CT/installation/debian-trixie`. Ce sous-repertoire installe un composant distinct dans `/opt/lpe-ct` avec sa propre interface de management, sans exposer le back office coeur sur le serveur DMZ, et provisionne aussi un binaire `Magika` CLI epingle dans `/opt/lpe-ct/bin/magika` pour la validation SMTP entrante.

Les scripts `LPE-CT` installent aussi un listener SMTP, un spool local dans `/var/spool/lpe-ct`, et trois jeux de tests:

L'integration fonctionnelle `LPE` / `LPE-CT` demande aussi d'aligner `LPE_CT_CORE_DELIVERY_BASE_URL`, `LPE_CT_API_BASE_URL` et `LPE_INTEGRATION_SHARED_SECRET` entre les deux noeuds. `LPE_INTEGRATION_SHARED_SECRET` est maintenant obligatoire des deux cotes au demarrage, doit rester hors des interfaces publiques, et doit etre definie avec une valeur forte non triviale d'au moins `32` caracteres. Le contrat est documente dans `docs/architecture/lpe-ct-integration.md`.

- `test-local-lpe-ct.sh` depuis le serveur `LPE-CT`
- `test-from-lpe.sh` depuis le LAN ou le serveur coeur
- `test-from-internet.sh` depuis une machine externe

### Preparation initiale sur un serveur Debian nu

Mettre a jour l'index APT et installer les outils minimaux pour recuperer le depot:

```bash
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl git
```

Cloner ensuite le depot avec un checkout Git sparse limite aux scripts Debian:

```bash
git clone --filter=blob:none --no-checkout https://github.com/dducret/LPE ~/LPE-bootstrap
cd ~/LPE-bootstrap
git sparse-checkout init --cone
git sparse-checkout set installation/debian-trixie
git checkout main
cd installation/debian-trixie
chmod +x *.sh
```

Ce clonage initial dans `~/LPE-bootstrap` est un bootstrap documentaire et operatoire limite aux scripts Debian. Le produit final n'est pas execute depuis ce repertoire.

Les scripts `install-lpe.sh` et `update-lpe.sh` enregistrent automatiquement `/opt/lpe/src` comme `safe.directory` Git lorsqu'ils sont executes en root.

Fichiers:

- `install-lpe.sh` installe les prerequis, clone le depot, compile `lpe-cli` et installe le service systemd
- `install-lpe.sh` provisionne aussi le binaire `Magika` CLI epingle, verifie par checksum, dans `/opt/lpe/bin/magika`
- `install-lpe.sh` demarre aussi `lpe.service` a la fin de l'installation
- `install-lpe.sh` installe aussi `nodejs`, `npm` et `nginx`, build `web/admin` et `web/client`, deploie les interfaces statiques et active le site `nginx`
- `update-lpe.sh` met a jour le depot, applique les migrations SQL, recompile `lpe-cli` et redemarre le service
- `update-lpe.sh` reprovisionne aussi la meme version epinglee de `Magika` pour garder une detection deterministe
- `update-lpe.sh` rebuild aussi `web/admin` et `web/client`, redeploie les assets statiques et recharge `nginx`
- `bootstrap-postgresql.sh` cree un role et une base PostgreSQL
- `bootstrap-postgresql.sh` installe aussi PostgreSQL serveur si necessaire puis le demarre
- les scripts d'installation utilisent le binaire `rustup` disponible dans le systeme puis initialisent le toolchain `stable` avant compilation
- `run-migrations.sh` applique les migrations SQL PostgreSQL du projet
- `run-migrations.sh` applique aussi le schema persistant de la console d'administration
- `check-lpe.sh` verifie l'installation, PostgreSQL, le service et les endpoints HTTP
- `lpe.service` decrit le service systemd initial
- `lpe.nginx.conf` sert de template pour le site `nginx` de la console d'administration
- `lpe.env.example` fournit une base de configuration

Ordre recommande:

1. executer `bootstrap-postgresql.sh`
2. executer `install-lpe.sh`
3. ajuster `/etc/lpe/lpe.env`
4. executer `run-migrations.sh`
5. verifier le service avec `systemctl status lpe.service`
6. ouvrir `http://adresse-du-serveur/` pour acceder a la console d'administration via `nginx`
7. ouvrir `http://adresse-du-serveur/mail/` pour acceder au client web

Le client web demande une authentification utilisateur. Creer d'abord un compte et son mot de passe depuis la page domaine de l'administration, puis se connecter a `/mail/` avec l'adresse email complete et ce mot de passe.

La console d'administration enregistre desormais ses comptes, mots de passe de comptes, boites, demandes d'import/export `PST`, domaines, alias, parametres, administrateurs delegues, objets antispam et evenements d'audit dans `PostgreSQL`. L'execution des migrations n'est donc plus optionnelle apres deploiement ou mise a jour du schema. `update-lpe.sh` les applique automatiquement apres le `git pull`, afin d'eviter de deployer une API plus recente que le schema PostgreSQL.

Les imports `PST` peuvent etre envoyes depuis le navigateur. Le service valide d'abord chaque fichier entrant avec Google `Magika`, puis stocke les fichiers recus dans `LPE_PST_IMPORT_DIR`, par defaut `/var/lib/lpe/imports`, et cree la demande d'import `PST` avec le chemin serveur obtenu. La taille maximale acceptee par l'API est configuree par `LPE_PST_UPLOAD_MAX_BYTES`, par defaut `21474836480` octets. Le reverse proxy `nginx` est aligne avec `LPE_NGINX_CLIENT_MAX_BODY_SIZE`, par defaut `20g`. Le chemin du binaire est configure via `LPE_MAGIKA_BIN`, par defaut `/opt/lpe/bin/magika`, et le seuil minimal via `LPE_MAGIKA_MIN_SCORE`.

La premiere connexion ne cree plus d'administrateur automatiquement. Le bootstrap admin est maintenant explicite: definir temporairement `LPE_BOOTSTRAP_ADMIN_EMAIL`, `LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME` (optionnel) et `LPE_BOOTSTRAP_ADMIN_PASSWORD` avec un mot de passe fort d'au moins `12` caracteres, puis executer `lpe-cli bootstrap-admin` sur le serveur coeur avant exposition de la console. Si un administrateur existe deja, la commande echoue sans modifier l'etat.

Exemple complet:

```bash
cd ~/LPE-bootstrap/installation/debian-trixie
./bootstrap-postgresql.sh
./install-lpe.sh
nano /etc/lpe/lpe.env
./run-migrations.sh
LPE_BOOTSTRAP_ADMIN_EMAIL=admin@example.test LPE_BOOTSTRAP_ADMIN_PASSWORD='Very-Strong-Bootstrap-Password-2026' /opt/lpe/bin/lpe-cli bootstrap-admin
systemctl status lpe.service
./check-lpe.sh
```

Par defaut:

- `lpe.service` ecoute sur `127.0.0.1:8080`
- `nginx` expose la console d'administration sur le port `80`
- `nginx` expose le client web sur `/mail/`
- `nginx` reverse-proxy `/api/` vers le service Rust local

Si `LPE_BIND_ADDRESS` ou `LPE_SERVER_NAME` changent dans `/etc/lpe/lpe.env`, relancer `update-lpe.sh` pour regenerer la configuration `nginx`.

Pour les mises a jour ulterieures:

1. pousser le commit voulu dans `https://github.com/dducret/LPE`
2. executer `update-lpe.sh`

`update-lpe.sh` execute `run-migrations.sh` automatiquement. Cela couvre notamment les changements de schema utilises par `/api/mail/workspace`, comme les tables `contacts` et `calendar_events`.

Si tu veux d'abord recuperer les derniers scripts avant une mise a jour:

```bash
cd /opt/lpe/src
git pull --ff-only origin main
cd installation/debian-trixie
./update-lpe.sh
./check-lpe.sh
```

Pour valider l'installation:

1. executer `check-lpe.sh`

## English

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
- `lpe.service` describes the initial systemd service
- `lpe.nginx.conf` is the template used to generate the administration `nginx` site
- `lpe.env.example` provides a base configuration

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

The first sign-in no longer creates an administrator automatically. Admin bootstrap is now explicit: set `LPE_BOOTSTRAP_ADMIN_EMAIL`, optional `LPE_BOOTSTRAP_ADMIN_DISPLAY_NAME`, and `LPE_BOOTSTRAP_ADMIN_PASSWORD` temporarily with a strong password of at least `12` characters, then run `lpe-cli bootstrap-admin` on the core server before exposing the console. If an administrator already exists, the command fails without changing the state.

Complete example:

```bash
cd ~/LPE-bootstrap/installation/debian-trixie
./bootstrap-postgresql.sh
./install-lpe.sh
nano /etc/lpe/lpe.env
./run-migrations.sh
LPE_BOOTSTRAP_ADMIN_EMAIL=admin@example.test LPE_BOOTSTRAP_ADMIN_PASSWORD='Very-Strong-Bootstrap-Password-2026' /opt/lpe/bin/lpe-cli bootstrap-admin
systemctl status lpe.service
./check-lpe.sh
```

By default:

- `lpe.service` listens on `127.0.0.1:8080`
- `nginx` exposes the administration console on port `80`
- `nginx` exposes the web client on `/mail/`
- `nginx` reverse-proxies `/api/` to the local Rust service

If `LPE_BIND_ADDRESS` or `LPE_SERVER_NAME` changes in `/etc/lpe/lpe.env`, run `update-lpe.sh` again to regenerate the `nginx` configuration.

For later updates:

1. push the desired commit to `https://github.com/dducret/LPE`
2. run `update-lpe.sh`

`update-lpe.sh` runs `run-migrations.sh` automatically. This covers schema changes used by `/api/mail/workspace`, such as the `contacts` and `calendar_events` tables.

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
