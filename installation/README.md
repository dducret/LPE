# Installation

## Debian Trixie

Le repertoire `debian-trixie` prepare une installation source de `LPE` depuis:

- `https://github.com/dducret/LPE`

Fichiers:

- `install-lpe.sh` installe les prerequis, clone le depot, compile `lpe-cli` et installe le service systemd
- `update-lpe.sh` met a jour le depot, recompile `lpe-cli` et redemarre le service
- `bootstrap-postgresql.sh` cree un role et une base PostgreSQL
- `lpe.service` decrit le service systemd initial
- `lpe.env.example` fournit une base de configuration

Ordre recommande:

1. executer `bootstrap-postgresql.sh`
2. executer `install-lpe.sh`
3. ajuster `/etc/lpe/lpe.env`
4. lancer `systemctl start lpe.service`

Pour les mises a jour ulterieures:

1. pousser le commit voulu dans `https://github.com/dducret/LPE`
2. executer `update-lpe.sh`
