# Task 78: continuous driver archives

This feature is off unless `RAY_DRIVER_ARCHIVE_ENABLED=1`. It requires the
matching Apodex Ray 2.55.1 producer/reader patch; do not enable a collector by
itself. Non-S3 backends are rejected when enabled. No production image is selected
or published by this change.

## Protocol and completion

The producer writes an atomic marker in `<session>/logs/.driver-archive-v2/`
and holds a nonblocking shared Linux flock on the append-only driver file. The
open file description travels with inherited stdout/stderr, including children
that outlive the Job's terminal status. The supervisor seals only when it stops
writing; the manager also tracks/seals its own setup/diagnostic file. A failed
handshake invalidates an existing marker. This does not add S3 calls or waits for
locks to job execution.

A file is complete only when its marker is sealed, an exclusive nonblocking
flock proves there are no cooperating writers, and all observed EOF bytes are
committed. `SUCCEEDED`, a quiet file, or collector SIGTERM alone is insufficient.
Arbitrary overwrite/copytruncate and third-party writes bypassing this producer
protocol are unsupported. Observed truncation, identity or completed-file mtime
changes poison the index; this is detection, not restoration of lost bytes.

Under the existing cluster prefix:

```
archive-v2/<node-id>/<submission-id>/index.json
archive-v2/<node-id>/<submission-id>/<generation>/chunks/<sha256>
archive-v2/<node-id>/<submission-id>/<generation>/checkpoints/<sha256>
```

A checkpoint records `{version,start,end,data,previous}`. Chunks and checkpoints
are immutable conditional creates. The index advances only after those writes
succeed, using S3 `If-Match` on its prior ETag (or `If-None-Match: *` on creation).
ETags provide concurrency control; SHA-256 validates bytes. Remote committed
progress, not an in-memory offset, is authoritative after a restart/lost reply.
The Go v1 SDK transmits conditional headers before signing; HTTP tests cover them.
Real bucket behavior remains part of authorized deployment acceptance.

Each step transfers at most 1 MiB. Tiny chunks are compacted once after closure
when count exceeds `ceil(bytes/1MiB)+8`: the old chain remains authoritative until
the bounded replacement chain is complete. This may read/upload the file once
more and retain roughly twice the payload until lifecycle expiry. It avoids
unbounded repeated whole-file snapshots and thousands of GETs for a small log.

`readable_until` is initial index creation + 364 days, never extended on append.
It deliberately precedes the existing 365-day per-object lifecycle. A Job lasting
beyond this horizon cannot be promised a complete archive. GC/registrar must
remain disabled until they understand the format. Do not individually delete
referenced chunks. This change does not modify lifecycle or permissions.

## Scheduling, shutdown, failures

- One serial worker, 10-second ticks, at most 4 steps / 4 MiB per tick, at most
  512 markers examined. Successful append normally costs one GET and three PUTs;
  four changing files imply about 1.6 requests/s per Pod, excluding initial
  creates, compaction and retries. An unchanged completed file makes no request.
- Scan operations have a 20-second context. Retry backoff is 2–64 seconds plus
  up to 1 second of jitter. Cache cap: 4096 tracked files; open directories cap:
  256. Directory cursors advance and directories rotate across ticks. Hitting a
  directory/tracking limit is logged; limits are not an all-files health PASS.
- Shutdown cancels the current scan/managed previous-session flush, then gives
  **all driver logs together 30 seconds**, including waiting for file moves.
  This pass precedes ordinary legacy/system uploads. Kubernetes may allow less
  time; legacy endpoint/system handling retains its existing lifecycle. No claim
  is made that the entire legacy collector always exits within 30 seconds.
- `prev-logs` upload failures retain source files. Only verified successful files
  are moved, and only empty directories removed. Legacy uploads now use bounded
  streaming instead of allocating the whole file. Managed driver files cannot
  be promoted to legacy archives until v2 completion is confirmed.
- v1 full objects are best-effort compatibility copies in the existing final/
  previous-session paths. They are not required to commit v2. The standalone
  History Server is **not** upgraded to read v2 by this PR.

Scan logs expose `driver_archive_tracked_pending_bytes`,
`driver_archive_tracked_pending_files`, `driver_archive_oldest_tracked_pending_seconds`,
`driver_archive_tracking_limit_reached`, examined/attempted/failed counts.
These are bounded cached observations, not full disk usage or an exact global
oldest-byte timestamp. The Ray image's `archive_tool.py status` queries current
local/S3 byte counts for explicitly scoped files; `verify` checks actual bytes.
Use verification before planned restarts; scan counts alone cannot authorize one.

Loss of `emptyDir` can still lose the unuploaded tail. An outage lasting longer
than local storage or Pod lifetime cannot be repaired by retry logic. Incomplete,
corrupt, expired or denied archives must remain explicit reader errors.

## Local validation

```
cd historyserver
go test -race -p=1 ./pkg/collector/driverarchive ./pkg/collector/logcollector/runtime/logcollector ./pkg/storage/s3
go vet ./pkg/collector/driverarchive ./pkg/collector/logcollector/runtime/... ./pkg/storage/s3 ./cmd/collector
go build -o /tmp/task78-collector ./cmd/collector
```

Tests cover active writers, inherited writer lifetime on the Python side, empty
and multiblock files, truncation/replacement, failed chunks/checkpoints, response
loss after a committed append, resumption, compaction, deadlines and failed-file
retention. `TASK78_WIRE_FIXTURE=/tmp/objects.json go test -run TestExportWireFixture
./pkg/collector/driverarchive` emits the Go wire fixture used by Spectrum tests.
