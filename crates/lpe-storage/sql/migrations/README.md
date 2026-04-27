# LPE Schema Migrations

`update-lpe.sh` applies SQL files from this directory in lexicographic order.

Each migration must be non-destructive by default, must preserve tenant-owned
mailbox and collaboration data, and must update `schema_metadata.schema_version`
when it changes the expected runtime schema.

Fresh installs still use `crates/lpe-storage/sql/schema.sql` through
`installation/debian-trixie/init-schema.sh`, which records bundled migrations as
already applied because the canonical schema includes them.
