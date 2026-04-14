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
- `update-lpe.sh` met a jour le depot, recompile `lpe-cli` et redemarre le service
- `bootstrap-postgresql.sh` cree un role et une base PostgreSQL
- `bootstrap-postgresql.sh` installe aussi PostgreSQL serveur si necessaire puis le demarre
- les scripts d'installation utilisent le binaire `rustup` disponible dans le systeme puis initialisent le toolchain `stable` avant compilation
- `run-migrations.sh` applique les migrations SQL PostgreSQL du projet
- `check-lpe.sh` verifie l'installation, PostgreSQL, le service et les endpoints HTTP
- `lpe.service` decrit le service systemd initial
- `lpe.env.example` fournit une base de configuration

Ordre recommande:

1. executer `bootstrap-postgresql.sh`
2. executer `install-lpe.sh`
3. ajuster `/etc/lpe/lpe.env`
4. executer `run-migrations.sh`
5. lancer `systemctl start lpe.service`

Exemple complet:

```bash
cd ~/LPE-bootstrap/installation/debian-trixie
./bootstrap-postgresql.sh
./install-lpe.sh
nano /etc/lpe/lpe.env
./run-migrations.sh
systemctl start lpe.service
./check-lpe.sh
```

Pour les mises a jour ulterieures:

1. pousser le commit voulu dans `https://github.com/dducret/LPE`
2. executer `update-lpe.sh`
3. executer `run-migrations.sh` si le schema PostgreSQL a change

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
- `update-lpe.sh` updates the repository, rebuilds `lpe-cli`, and restarts the service
- `bootstrap-postgresql.sh` creates a PostgreSQL role and database
- `bootstrap-postgresql.sh` also installs the PostgreSQL server if needed and starts it
- the installation scripts use the system `rustup` binary and initialize the `stable` toolchain before building
- `run-migrations.sh` applies the project's PostgreSQL SQL migrations
- `check-lpe.sh` verifies the installation, PostgreSQL, the service, and the HTTP endpoints
- `lpe.service` describes the initial systemd service
- `lpe.env.example` provides a base configuration

Recommended order:

1. run `bootstrap-postgresql.sh`
2. run `install-lpe.sh`
3. adjust `/etc/lpe/lpe.env`
4. run `run-migrations.sh`
5. start `systemctl start lpe.service`

Complete example:

```bash
cd ~/LPE-bootstrap/installation/debian-trixie
./bootstrap-postgresql.sh
./install-lpe.sh
nano /etc/lpe/lpe.env
./run-migrations.sh
systemctl start lpe.service
./check-lpe.sh
```

For later updates:

1. push the desired commit to `https://github.com/dducret/LPE`
2. run `update-lpe.sh`
3. run `run-migrations.sh` if the PostgreSQL schema changed

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
