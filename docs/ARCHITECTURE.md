# Architecture

## Overview

Video Digest Server is a concurrent HTTP ingest server written in Rust.

The target architecture is intentionally small:

```text
HTTP API -> Upload Stream -> Hash + Temp File -> Dedup Registry -> Job Queue -> Worker -> FFmpeg -> Artifact
```

The system does not use a database. All runtime state is in memory.

This document describes the target design. The implementation is not yet present in this repository.

---

## Components

### 1. HTTP API

Responsibilities:

- accept multipart uploads
- validate profile name
- stream file body
- return job response
- expose job status
- expose artifact download

Expected framework:

- Axum
- Tokio

### 2. Upload Handler

Responsibilities:

- read multipart file stream
- write bytes to a temporary file
- compute SHA-256 while reading the stream
- finalize hash only after the upload completes
- pass finalized hash to dedup registry

The upload handler must not insert a job before the upload completes.

### 3. Dedup Registry

Responsibilities:

- maintain mapping from `DedupKey` to `DedupEntry`
- ensure only one canonical job exists for each `(content_hash, profile_name)`
- prevent check-then-insert race

Target structure:

```rust
struct RegistryState {
    dedup: HashMap<DedupKey, DedupEntry>,
    jobs: HashMap<JobId, Job>,
}
```

Target synchronization:

```rust
Arc<tokio::sync::Mutex<RegistryState>>
```

### 4. Job Queue

Responsibilities:

- accept only new canonical jobs
- provide jobs to workers
- prevent duplicate processing

The queue should not receive deduplicated requests.

### 5. Worker Pool

Responsibilities:

- receive jobs from queue
- update status from `queued` to `running`
- execute FFmpeg command
- update status to `succeeded` or `failed`

### 6. FFmpeg Runner

Responsibilities:

- read profile arguments from YAML profile config
- build command using argument array
- run FFmpeg as an external process
- record success or failure

The client never sends raw FFmpeg arguments.

### 7. Local Storage

Responsibilities:

- store temporary upload files
- store canonical job input files
- store output artifacts

Target layout:

```text
data/
  tmp/
  jobs/
    {job_id}/
      input/
      output/
```

---

## Shared State

| State | Owner | Synchronization |
|---|---|---|
| dedup map | `RegistryState` | Mutex |
| job map | `RegistryState` | Mutex |
| job queue | Queue component | async channel / mutex |
| profiles | loaded at startup | immutable shared reference |
| local files | filesystem | unique paths by job ID |

---

## Locking Rules

Lock should be held only for small shared-state operations.

Allowed under lock:

- check dedup key
- insert dedup entry
- insert job metadata
- update job status
- read job metadata

Not allowed under lock:

- reading upload stream
- hashing large files
- FFmpeg execution
- artifact download
- long polling

---

## Job State Machine

```text
queued -> running -> succeeded
queued -> running -> failed
```

Invalid transitions must be rejected.
