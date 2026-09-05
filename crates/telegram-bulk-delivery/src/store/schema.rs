use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::Path;
use thiserror::Error;

use crate::MIN_SQLITE_VERSION_NUMBER;

pub const MIGRATION: &str = include_str!("migrations/0001_init.sql");
pub const MIGRATION_0002: &str = include_str!("migrations/0002_webhook_pages.sql");
pub const MIGRATION_0003: &str = include_str!("migrations/0003_lease_token.sql");
pub const MIGRATION_0004: &str = include_str!("migrations/0004_telegram_api_base.sql");
pub const LATEST_MIGRATION_VERSION: i64 = 4;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("sqlite runtime {actual} is below required 3.51.3")]
    SQLiteTooOld { actual: String },
    #[error("sqlite invariant failed: {0}")]
    Invariant(&'static str),
    #[error("store worker is unavailable")]
    WorkerUnavailable,
    #[error("store response channel closed")]
    ResponseClosed,
    #[error("invalid writer command: {0}")]
    InvalidCommand(&'static str),
    #[error("operator ceiling exceeded: {0}")]
    CapacityExceeded(&'static str),
    #[error("database schema version {version} is newer than this binary (supports up to {latest})")]
    SchemaTooNew { version: i64, latest: i64 },
}

pub fn open_writer(path: &Path) -> Result<Connection, StoreError> {
    let conn = Connection::open(path)?;
    configure_common(&conn)?;
    let mode: String =
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::Invariant("journal_mode must be WAL"));
    }
    verify(&conn, false)?;
    Ok(conn)
}

pub fn open_reader(path: &Path) -> Result<Connection, StoreError> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    configure_common(&conn)?;
    conn.pragma_update(None, "query_only", "ON")?;
    verify(&conn, true)?;
    Ok(conn)
}

fn configure_common(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA wal_autocheckpoint=0; PRAGMA temp_store=FILE; PRAGMA mmap_size=0; PRAGMA cache_size=-8000; PRAGMA recursive_triggers=ON;")?;
    Ok(())
}

pub fn verify(conn: &Connection, reader: bool) -> Result<(), StoreError> {
    let actual: String = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    let runtime_number = parse_version_number(&actual).ok_or(StoreError::SQLiteTooOld {
        actual: actual.clone(),
    })?;
    if runtime_number < MIN_SQLITE_VERSION_NUMBER {
        return Err(StoreError::SQLiteTooOld { actual });
    }
    let mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    let synchronous: i64 = conn.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    let foreign_keys: i64 = conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    let temp_store: i64 = conn.pragma_query_value(None, "temp_store", |row| row.get(0))?;
    let mmap_size: i64 = conn.pragma_query_value(None, "mmap_size", |row| row.get(0))?;
    let busy_timeout: i64 = conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::Invariant("journal_mode must be WAL"));
    }
    if synchronous != 2 {
        return Err(StoreError::Invariant("synchronous must be FULL"));
    }
    if foreign_keys != 1 {
        return Err(StoreError::Invariant("foreign_keys must be ON"));
    }
    if temp_store != 1 {
        return Err(StoreError::Invariant("temp_store must be FILE"));
    }
    if mmap_size != 0 {
        return Err(StoreError::Invariant("mmap_size must be zero"));
    }
    if busy_timeout != 5000 {
        return Err(StoreError::Invariant("busy_timeout must be 5000"));
    }
    if reader {
        let query_only: i64 = conn.pragma_query_value(None, "query_only", |row| row.get(0))?;
        if query_only != 1 {
            return Err(StoreError::Invariant("reader must be query-only"));
        }
    }
    Ok(())
}

/// Highest applied schema version, or 0 when `_migrations` is absent.
pub fn current_version(conn: &Connection) -> Result<i64, rusqlite::Error> {
    let has: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_migrations'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !has {
        return Ok(0);
    }
    conn.query_row(
        "SELECT coalesce(max(version),0) FROM _migrations",
        [],
        |r| r.get(0),
    )
}

/// Apply pending migrations in order. Idempotent; safe to run on every boot.
pub fn migrate(conn: &mut Connection) -> Result<(), StoreError> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| {
        let version = current_version(conn)?;
        // Fail fast rather than running an older binary against a newer DB:
        // the writer's column-count insert would otherwise fail later with a
        // confusing error on the first config write instead of at boot.
        if version > LATEST_MIGRATION_VERSION {
            return Err(StoreError::SchemaTooNew {
                version,
                latest: LATEST_MIGRATION_VERSION,
            });
        }
        if version == 0 {
            conn.execute_batch(MIGRATION)?;
            conn.execute(
                "INSERT INTO _migrations(version,name,applied_at_unix) VALUES(1,'init',unixepoch())",
                [],
            )?;
        }
        if version < 2 {
            conn.execute_batch(MIGRATION_0002)?;
            conn.execute(
                "INSERT INTO _migrations(version,name,applied_at_unix) VALUES(2,'webhook_pages',unixepoch())",
                [],
            )?;
        }
        if version < 3 {
            conn.execute_batch(MIGRATION_0003)?;
            conn.execute(
                "INSERT INTO _migrations(version,name,applied_at_unix) VALUES(3,'lease_token',unixepoch())",
                [],
            )?;
        }
        if version < 4 {
            conn.execute_batch(MIGRATION_0004)?;
            conn.execute(
                "INSERT INTO _migrations(version,name,applied_at_unix) VALUES(4,'telegram_api_base',unixepoch())",
                [],
            )?;
        }
        Ok::<_, StoreError>(())
    })();
    match result {
        Ok(()) => match conn.execute_batch("COMMIT") {
            Ok(()) => Ok(()),
            Err(commit_error) => {
                // A COMMIT failure may leave the transaction open; roll back so
                // the connection can run migrations again on the next boot.
                let _ = conn.execute_batch("ROLLBACK");
                Err(commit_error.into())
            }
        },
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error.into())
        }
    }
}

fn parse_version_number(version: &str) -> Option<i32> {
    let mut parts = version.split('.');
    let major: i32 = parts.next()?.parse().ok()?;
    let minor: i32 = parts.next()?.parse().ok()?;
    let patch: i32 = parts.next()?.parse().ok()?;
    if parts.next().is_some()
        || major < 0
        || !(0..=999).contains(&minor)
        || !(0..=999).contains(&patch)
    {
        return None;
    }
    major
        .checked_mul(1_000_000)?
        .checked_add(minor.checked_mul(1_000)?)?
        .checked_add(patch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use tempfile::tempdir;

    #[test]
    fn migration_v3_to_v4_adds_telegram_api_base_column() {
        let tmp = tempdir().unwrap();
        let conn = open_writer(&tmp.path().join("test.db")).unwrap();

        // Construct a v3 DB: apply 0001-0003 with matching _migrations rows.
        conn.execute_batch(MIGRATION).unwrap();
        conn.execute(
            "INSERT INTO _migrations(version,name,applied_at_unix) VALUES(1,'init',unixepoch())",
            [],
        )
        .unwrap();
        conn.execute_batch(MIGRATION_0002).unwrap();
        conn.execute(
            "INSERT INTO _migrations(version,name,applied_at_unix) VALUES(2,'webhook_pages',unixepoch())",
            [],
        ).unwrap();
        conn.execute_batch(MIGRATION_0003).unwrap();
        conn.execute(
            "INSERT INTO _migrations(version,name,applied_at_unix) VALUES(3,'lease_token',unixepoch())",
            [],
        ).unwrap();

        // Now we have a v3 DB. Seed a bots row (FK) + a 20-column bot_configs row.
        let bot_id = [7u8; 32];
        conn.execute(
            "INSERT INTO bots (bot_id, token_nonce, token_ciphertext, token_kid, telegram_user_id, created_at_unix, last_seen_unix) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![&bot_id[..], &[1u8; 12], &[2u8; 32], "kid1", 12345i64, 1000i64, 1000i64],
        ).unwrap();

        conn.execute(
            "INSERT INTO bot_configs (
                bot_id, config_version, target_msgs_per_sec, retry_max_attempts,
                retry_base_ms, retry_max_ms, retry_jitter, retryable_classes_json,
                ambiguity_policy, job_deadline_secs, fairness_weight,
                webhook_url, webhook_https_only,
                webhook_secret_nonce, webhook_secret_ciphertext, webhook_secret_kid,
                webhook_max_attempts, webhook_retry_base_ms, webhook_retry_max_ms,
                updated_at_unix
            ) VALUES (?1, 1, 20.0, 8, 500, 60000, 'full', '[]', 'at_least_once', 86400, 1, NULL, 1, NULL, NULL, NULL, 10, 1000, 300000, 2000)",
            params![&bot_id[..]],
        ).unwrap();

        // Run the version-4 migration block (same as migrate() for version < 4).
        conn.execute_batch(MIGRATION_0004).unwrap();
        conn.execute(
            "INSERT INTO _migrations(version,name,applied_at_unix) VALUES(4,'telegram_api_base',unixepoch())",
            [],
        ).unwrap();

        // Verify the column exists.
        let col_exists: bool = conn
            .query_row(
                "SELECT 1 FROM pragma_table_info('bot_configs') WHERE name='telegram_api_base'",
                [],
                |_| Ok(true),
            )
            .optional()
            .unwrap()
            .unwrap_or(false);
        assert!(
            col_exists,
            "telegram_api_base column should exist after v4 migration"
        );

        // Pre-existing row reads NULL for the new column.
        let api_base: Option<String> = conn
            .query_row(
                "SELECT telegram_api_base FROM bot_configs WHERE bot_id = ?1",
                [&bot_id[..]],
                |r| r.get(0),
            )
            .optional()
            .unwrap()
            .flatten();
        assert!(
            api_base.is_none(),
            "pre-existing row should read NULL for telegram_api_base"
        );

        // _migrations records version 4.
        let max_version: i64 = conn
            .query_row("SELECT max(version) FROM _migrations", [], |r| r.get(0))
            .unwrap();
        assert_eq!(max_version, 4, "_migrations should record version 4");

        // Also verify current_version() reads 4.
        let cv = current_version(&conn).unwrap();
        assert_eq!(
            cv, 4,
            "current_version() should return 4 after v4 migration"
        );
    }
}
