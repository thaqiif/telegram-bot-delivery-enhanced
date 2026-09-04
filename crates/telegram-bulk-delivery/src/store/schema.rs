use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::Path;
use thiserror::Error;

use crate::MIN_SQLITE_VERSION_NUMBER;

pub const MIGRATION: &str = include_str!("migrations/0001_init.sql");
pub const MIGRATION_0002: &str = include_str!("migrations/0002_webhook_pages.sql");
pub const MIGRATION_0003: &str = include_str!("migrations/0003_lease_token.sql");
pub const LATEST_MIGRATION_VERSION: i64 = 3;

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
        Ok::<_, rusqlite::Error>(())
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
