# Versioned database migrations

This directory is reserved for ordered, immutable schema migrations.

The current authoritative runtime schema is still defined by
`smart_routing/common_vrp_db.py`, including `init_schema()` and its guarded
compatibility `ALTER TABLE` statements. No SQL migration has been copied here,
because an unwired duplicate would create a second schema authority.

When migrations are introduced, each migration must have a version, checksum,
idempotency or explicit one-time semantics, development evidence, production
backup/rollback instructions, and an entry in a schema migration history table.

Migration filenames must use `VNNN__lowercase_description.sql`. A release must
construct an explicit `MigrationSpec` allowlist containing the expected SHA-256,
description, and rollback metadata. Merely placing a SQL file in this directory
does not make it executable. `admin_tools.db.release_backend` validates the
allowlist, statement types, forbidden primitives, checksum, typed target
confirmation, advisory lock, timeout, transaction, and history semantics.
