# Installation

## Francais

### Debian Trixie

Le repertoire `debian-trixie` prepare une installation source de `LPE` depuis:

- `https://github.com/dducret/LPE`

Repertoire produit par defaut:

- `/opt/lpe`

Fichiers:

- `install-lpe.sh` installe les prerequis, clone le depot, compile `lpe-cli` et installe le service systemd
- `update-lpe.sh` met a jour le depot, recompile `lpe-cli` et redemarre le service
- `bootstrap-postgresql.sh` cree un role et une base PostgreSQL
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

Pour les mises a jour ulterieures:

1. pousser le commit voulu dans `https://github.com/dducret/LPE`
2. executer `update-lpe.sh`
3. executer `run-migrations.sh` si le schema PostgreSQL a change

Pour valider l'installation:

1. executer `check-lpe.sh`

## English

### Debian Trixie

The `debian-trixie` directory prepares a source installation of `LPE` from:

- `https://github.com/dducret/LPE`

Default product directory:

- `/opt/lpe`

Files:

- `install-lpe.sh` installs prerequisites, clones the repository, builds `lpe-cli`, and installs the systemd service
- `update-lpe.sh` updates the repository, rebuilds `lpe-cli`, and restarts the service
- `bootstrap-postgresql.sh` creates a PostgreSQL role and database
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

For later updates:

1. push the desired commit to `https://github.com/dducret/LPE`
2. run `update-lpe.sh`
3. run `run-migrations.sh` if the PostgreSQL schema changed

To validate the installation:

1. run `check-lpe.sh`
