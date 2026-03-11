# SQLite Driver for Kine

Kine uses SQLite as its default storage backend. The SQLite driver is only available in CGO-enabled builds; builds without CGO will return an error if SQLite is selected.

## Endpoint Format

```
sqlite://<path-to-database>?<parameters>
```

If no endpoint is specified, kine uses the `sqlite` driver with the following default DSN:

```
./db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate
```

The equivalent explicit endpoint would be:

```
sqlite://./db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate
```

The `litestream` scheme can be used instead of `sqlite` to enable [Litestream](https://litestream.io/) compatibility mode, which automatically disables all WAL checkpointing and startup VACUUM (see below).

## DSN Parameters

### Standard go-sqlite3 Parameters

These are standard parameters supported by the [go-sqlite3](https://github.com/mattn/go-sqlite3) driver. Only the parameters used in the default DSN are listed here; see the go-sqlite3 documentation for the full list.

| Parameter | Default | Description |
|---|---|---|
| `_journal` | `WAL` | Sets the SQLite journal mode. `WAL` (Write-Ahead Logging) is strongly recommended for concurrent access. |
| `cache` | `shared` | Sets the cache mode. `shared` allows multiple connections within the same process to share a single cache. |
| `_busy_timeout` | `30000` | Time in milliseconds that SQLite will wait when a table is locked before returning `SQLITE_BUSY`. The default is 30 seconds. |
| `_txlock` | `immediate` | Sets the transaction locking mode. `immediate` acquires a write lock at the start of the transaction, reducing contention under concurrent writes. |

### Kine-Specific Parameters

Kine recognizes the following custom flags in the DSN. These are detected by checking if the DSN string contains the flag name (they do not require a value — their mere presence enables the behavior).

| Parameter | Description |
|---|---|
| `_kine_disable_compact_wal_checkpoint` | Disables WAL checkpointing in two places: the `PRAGMA wal_checkpoint(TRUNCATE)` that runs at startup, and the `PRAGMA wal_checkpoint(FULL)` that runs after each compaction operation. By default, kine performs both of these checkpoints to keep the WAL file from growing unbounded. |
| `_kine_disable_wal_autocheckpoint` | Disables SQLite's automatic WAL checkpointing by setting `PRAGMA wal_autocheckpoint = 0`. By default, SQLite automatically checkpoints after the WAL reaches 1000 pages. |
| `_kine_disable_startup_vacuum` | Disables the `VACUUM` command that kine runs at startup to reclaim unused disk space. On very large databases the startup VACUUM can be slow, so this flag allows skipping it. |

> **Note:** When using the `litestream` driver, all three flags above are implicitly enabled and do not need to be set manually.

## Examples

**Default (no endpoint specified):**
```
kine
```

**Custom database path:**
```
kine --endpoint 'sqlite:///var/lib/kine/db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate'
```

**Disable startup VACUUM (useful for large databases):**
```
kine --endpoint 'sqlite:///var/lib/kine/db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate&_kine_disable_startup_vacuum'
```

**Disable all WAL checkpointing (manual WAL management):**
```
kine --endpoint 'sqlite:///var/lib/kine/db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate&_kine_disable_compact_wal_checkpoint&_kine_disable_wal_autocheckpoint'
```

**Litestream compatibility (all checkpointing and VACUUM disabled automatically):**
```
kine --endpoint 'litestream:///var/lib/kine/db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate'
```

## Startup Behavior

On startup, the SQLite driver performs the following steps in order:

1. **Schema creation** — Creates the `kine` table and indexes if they do not exist.
2. **WAL checkpoint** — Runs `PRAGMA wal_checkpoint(TRUNCATE)` unless `_kine_disable_compact_wal_checkpoint` is set.
3. **Disable auto-checkpoint** — Sets `PRAGMA wal_autocheckpoint = 0` if `_kine_disable_wal_autocheckpoint` is set.
4. **VACUUM** — Runs `VACUUM` to reclaim disk space, unless `_kine_disable_startup_vacuum` is set. A failure here is logged as a warning but is non-fatal.
5. **Migration** — Runs any pending schema migrations.
