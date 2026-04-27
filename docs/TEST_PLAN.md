# Test Plan

## Purpose

The tests are designed to prove that the server is safe under concurrent access to shared state.

The primary safety property:

```text
For the same (content_hash, profile), only one canonical job is created.
```

This document describes the planned tests. Tests will be implemented along with the server in subsequent PRs.

---

## Test Matrix

| Test | Level | Purpose |
|---|---|---|
| `dedup_registry_concurrent_access` | unit/integration | Verify atomic check-and-insert |
| `same_file_concurrent_upload` | integration | Verify HTTP-level dedup |
| `same_filename_different_content` | integration | Verify filename is not identity |
| `worker_no_duplicate_processing` | integration | Verify one job is not processed twice |
| `invalid_status_transition` | unit | Verify job state machine |
| `incomplete_upload_no_job` | integration | Verify dropped request does not create job |

---

## 1. `dedup_registry_concurrent_access`

### Setup

- create shared `RegistryState`
- create one `DedupKey`
- spawn 100 Tokio tasks
- every task calls `get_or_create(DedupKey)`

### Expected

- one canonical job
- all returned job IDs are equal
- dedup map size is 1

---

## 2. `same_file_concurrent_upload`

### Setup

- start test server
- prepare one small sample file
- spawn N HTTP clients
- every client uploads the same file and same profile

### Expected

- all successful responses contain the same `job_id`
- `deduplicated=false` appears once
- `deduplicated=true` appears N-1 times
- FFmpeg / mock transcoder is invoked once

---

## 3. `same_filename_different_content`

### Setup

- prepare two files with different bytes
- send both as original filename `sample.mp4`

### Expected

- `content_hash` differs
- `job_id` differs
- dedup does not use filename as identity

---

## 4. `worker_no_duplicate_processing`

### Setup

- create multiple jobs
- run multiple workers
- track processing count by `job_id`

### Expected

- each job is processed at most once
- terminal state is `succeeded` or `failed`
- invalid state transition does not occur

---

## 5. `incomplete_upload_no_job`

### Setup

- open connection
- begin upload
- drop connection before completion

### Expected

- no content hash finalized
- no dedup entry
- no job metadata
- no queue enqueue

---

## CI

Minimum required CI command:

```bash
cargo test --all
```

Optional:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```
