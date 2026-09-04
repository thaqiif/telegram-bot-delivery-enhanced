# Operations

## Deployment model

Run one service process against one SQLite database on **local persistent storage**. NFS/SMB/object-store mounts are unsupported. The reference VPS budget is 1 vCPU / 1 GiB RAM; the binary intentionally uses two Tokio workers, one SQLite writer OS thread, exactly four query-only SQLite reader threads, and at most eight blocking threads.

Set a stable 32-byte `BULK_MASTER_KEY` (base64) through a secret manager/environment and preserve it across reboots. Losing it makes encrypted bot tokens and webhook secrets undecryptable. Rotate deliberately with a migration; changing only `key_id` is not a rotation procedure.

The default bind is `127.0.0.1:8080`. Put a TLS reverse proxy in front, restrict `/metrics` to localhost/operator networks, apply client request/body timeouts longer than durable ingestion, and strip token-bearing paths from proxy logs (see `SECURITY.md`).

Example systemd unit limits:

```ini
[Service]
ExecStart=/opt/telegram-bulk-delivery/telegram-bulk-delivery /etc/telegram-bulk-delivery.toml
EnvironmentFile=/etc/telegram-bulk-delivery.env
Restart=on-failure
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/var/lib/telegram-bulk-delivery
MemoryMax=1G
MemoryHigh=896M
CPUQuota=100%
TasksMax=32
LimitNOFILE=4096
```

## SQLite invariants and the WAL

Startup rejects SQLite older than 3.51.3 and verifies:

- `journal_mode=WAL`
- `synchronous=FULL`
- `foreign_keys=ON`
- `temp_store=FILE`
- `mmap_size=0`
- `busy_timeout=5000`
- `wal_autocheckpoint=0`

All post-migration mutations are closed-set `WriterCmd` values on the single writer thread. Do not open a second writer in tooling while the service is live.

The maintenance task enqueues a writer-owned `wal_checkpoint(PASSIVE)` every second. At `wal_truncate_bytes` (64 MiB by default), it forces `wal_checkpoint(TRUNCATE)` even when Telegram grants are paused. `sqlite_wal_bytes` alarms at 32 MiB and should remain below 256 MiB after checkpoints. A long reader can make a checkpoint report busy; the next second retries. Persistent growth usually means a stuck/slow reader, slow disk, or writer saturation—not permission to weaken `synchronous=FULL`.

Monitor:

```promql
sqlite_wal_bytes > 33554432
writer_queue_depth >= 100
process_rss_bytes > 805306368
cgroup_memory_current_bytes > 805306368
oldest_nonterminal_recipient_age_seconds
```

## Retention and files

Completed jobs receive `expire_at_unix = completed_at_unix + 7 days`. Every `retention_sweep_secs` (30 seconds by default), one writer command deletes at most `retention_batch` (500) terminal jobs in expiry/job-ID order. Because sweeps are at most 900 seconds apart by validated configuration, removal occurs after seven days plus no more than fifteen minutes.

Retention **skips** jobs whose webhook state is `pending` or `delivering`. Terminal webhook rows/pages/delivery history are removed before the job to satisfy foreign keys; recipient/rate/file metadata cascades with the job. Blob files are removed after commit. A crash in that tiny interval can leave an orphan, which the next boot sweeps. Boot also removes abandoned temp directories and aborts `accepting` jobs left by a crash; only promoted jobs were acknowledged to clients.

## Safe backups and restores

Never copy the live SQLite main file alone: committed state can be in `-wal`, and a mismatched copy is not a backup. Use the writer-owned `Backup { destination }` command (`VACUUM INTO`) or stop the service cleanly and copy **the entire data directory** (DB, WAL/SHM if present, and `files/`) on the same-disk boundary.

For an online backup:

1. Ensure the destination is on trusted local storage and does not exist (`VACUUM INTO` refuses overwrite).
2. Execute the operator backup command through the writer.
3. Copy the resulting SQLite file and a consistent snapshot of `data/files/` to protected storage.
4. Verify the backup: `PRAGMA integrity_check` must return `ok`.
5. Test a restore periodically on a separate host/process with the same master key.

A SQLite backup without the referenced multipart blobs can restore queue metadata but cannot replay those deliveries.

## Disk-full and busy recovery

Admission should stop before `free_disk_reserve_bytes` (default 1 GiB) or `storage_high_watermark_bytes` is crossed. If SQLite reports `database or disk is full`:

1. Stop external traffic; do not delete `*.db-wal` or `*.db-shm`.
2. Free space outside the data directory or expand the volume.
3. Let the writer retry its checkpoint/command; the transaction that hit `SQLITE_FULL` was rolled back.
4. Run a writer-owned `TRUNCATE` checkpoint and a query-only `PRAGMA integrity_check`.
5. Resume admission after free-space/readiness checks are healthy.

The 5000 ms busy timeout lets short external locks clear. A writer blocked near five seconds indicates an unsupported concurrent writer or pathological filesystem latency.

## Process-kill and same-disk reboot drill

Run before production and after storage/runtime changes:

1. Point a staging process at a disposable local data directory and simulator upstream.
2. Submit one JSON job and one multipart job; wait for both HTTP 200 responses.
3. Confirm the multipart blob exists under `data/files/<job-id>/` and `job_files` references it.
4. `SIGKILL` the service (no graceful shutdown), preserving the full data directory including WAL/SHM.
5. Restart with the **same** database/files/master key and a new process epoch.
6. Confirm both accepted jobs become visible/runnable, the multipart dispatch reads the durable blob, and both complete without `integrity_check` errors.
7. For a recipient killed after `MarkWireStarted`, verify recovery follows the snapshotted ambiguity policy rather than claiming it was definitely unsent.
8. Deliver/dedupe the completion webhook and verify polling reaches `delivered` or explicit `exhausted`.

Automated coverage lives in `tests/us005.rs`; the operator scripts are in `crates/telegram-bulk-delivery/scripts/`.

## Resource-constrained verification

`crates/telegram-bulk-delivery/scripts/loadtest-1vcpu-soak.sh` is a **30-minute soak**, not a completion benchmark. It must run inside an already constrained cgroup/container (1 CPU, 1 GiB) and records `memory.current`/RSS, WAL, writer depth, and 1,000-bot fairness. Pass criteria: memory below 768 MiB, WAL below 256 MiB after checkpoints, bounded queues, and every ready bot receives a grant after at least 1,000 global grants.

`crates/telegram-bulk-delivery/scripts/loadtest-1vcpu-complete-100k.sh` is a separate, opt-in long completion proof. At the 25/s hard operator ceiling, 100,000 sends require at least 4,000 seconds (66.7 minutes), so its budget is 75 minutes. Never report a 20- or 30-minute 100k completion under the real limiter.

The scripts refuse to claim cgroup proof when CPU/memory constraints are absent. A local short/fake-clock test verifies mechanics; it is not substitute evidence for the 30-minute constrained soak.
