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

Pour un serveur de tri separe en `DMZ`, utiliser plutot `LPE-CT/installation/debian-trixie`. Ce sous-repertoire installe un composant distinct dans `/opt/lpe-ct` avec sa propre interface de management et sans exposer le back office coeur sur le serveur DMZ.

Les scripts `LPE-CT` installent aussi un listener SMTP, un spool local dans `/var/spool/lpe-ct`, et trois jeux de tests:

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
- `install-lpe.sh` demarre aussi `lpe.service` a la fin de l'installation
- `install-lpe.sh` installe aussi `nodejs`, `npm` et `nginx`, build `web/admin` et `web/client`, deploie les interfaces statiques et active le site `nginx`
- `update-lpe.sh` met a jour le depot, recompile `lpe-cli` et redemarre le service
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

La console d'administration enregistre desormais ses comptes, mots de passe de comptes, boites, demandes d'import/export `PST`, domaines, alias, parametres, administrateurs delegues, objets antispam et evenements d'audit dans `PostgreSQL`. L'execution des migrations n'est donc plus optionnelle apres deploiement ou mise a jour du schema.

La premiere connexion cree automatiquement un administrateur de bootstrap si aucun identifiant n'existe encore. Les variables `LPE_BOOTSTRAP_ADMIN_EMAIL`, `LPE_BOOTSTRAP_ADMIN_PASSWORD` et `LPE_ADMIN_SESSION_MINUTES` doivent etre ajustees dans `/etc/lpe/lpe.env` avant exposition de la console.

Exemple complet:

```bash
cd ~/LPE-bootstrap/installation/debian-trixie
./bootstrap-postgresql.sh
./install-lpe.sh
nano /etc/lpe/lpe.env
./run-migrations.sh
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
3. executer `run-migrations.sh` si le schema PostgreSQL a change, notamment pour les migrations de console d'administration comme `0006_admin_auth.sql`, `0007_pst_job_execution.sql` et `0008_account_credentials.sql`

Si tu veux d'abord recuperer les derniers scripts avant une mise a jour:

```bash
cd /opt/lpe/src
git pull --ff-only origin main
cd installation/debian-trixie
./update-lpe.sh
./run-migrations.sh
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

For a separate sorting server in the `DMZ`, use `LPE-CT/installation/debian-trixie` instead. That subdirectory installs a distinct component into `/opt/lpe-ct` with its own management UI and without exposing the core back office on the DMZ server.

The `LPE-CT` scripts also install an SMTP listener, a local spool in `/var/spool/lpe-ct`, and three test suites:

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
- `install-lpe.sh` also starts `lpe.service` at the end of the installation
- `install-lpe.sh` also installs `nodejs`, `npm`, and `nginx`, builds `web/admin` and `web/client`, deploys the static UIs, and enables the `nginx` site
- `update-lpe.sh` updates the repository, rebuilds `lpe-cli`, and restarts the service
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

The administration console now stores its accounts, account passwords, mailboxes, `PST` import/export requests, domains, aliases, settings, delegated administrators, anti-spam objects, and audit events in `PostgreSQL`. Running migrations is therefore mandatory after deployment or any schema update.

The first sign-in automatically creates a bootstrap administrator if no credential exists yet. `LPE_BOOTSTRAP_ADMIN_EMAIL`, `LPE_BOOTSTRAP_ADMIN_PASSWORD`, and `LPE_ADMIN_SESSION_MINUTES` must be adjusted in `/etc/lpe/lpe.env` before exposing the console.

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

If `LPE_BIND_ADDRESS` or `LPE_SERVER_NAME` changes in `/etc/lpe/lpe.env`, run `update-lpe.sh` again to regenerate the `nginx` configuration.

For later updates:

1. push the desired commit to `https://github.com/dducret/LPE`
2. run `update-lpe.sh`
3. run `run-migrations.sh` if the PostgreSQL schema changed, especially for administration console migrations such as `0006_admin_auth.sql`, `0007_pst_job_execution.sql`, and `0008_account_credentials.sql`

If you want to fetch the latest scripts first before an update:

```bash
cd /opt/lpe/src
git pull --ff-only origin main
cd installation/debian-trixie
./update-lpe.sh
./run-migrations.sh
./check-lpe.sh
```

To validate the installation:

1. run `check-lpe.sh`
