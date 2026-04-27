# Video Digest Server

Video Digest Server is a planned Rust/Tokio service for concurrent video uploads and FFmpeg transcoding.

When multiple clients upload the same video with the same profile, the target behavior is to create one canonical transcoding job and return that job ID to duplicate requests.

> Current status: design documentation only. Implementation will be added in later PRs.

## Core Idea

The service deduplicates work by this key:

```text
DedupKey = (content_sha256, profile_name)
```

The main concurrency issue is a logical check-then-insert race in an in-memory `HashMap`. This is an application-level race condition, not a Rust memory data race.

## System Overview

```mermaid
flowchart LR
    C["Concurrent Clients"] -->|"POST /api/jobs"| API["Axum HTTP API"]
    API --> U["Upload Stream"]
    U --> H["Temp File + SHA-256"]
    H --> R["RegistryState<br/>Arc&lt;Mutex&gt;"]
    R -->|"new canonical job"| Q["Job Queue"]
    Q --> W["Worker Pool"]
    W --> F["FFmpeg"]
    F --> A["Local Artifact"]
```

The target service is intentionally small: no database, no authentication, no session management, and no required UI. Runtime job state is kept in memory and is lost on restart.

## Race Condition

Naive check-then-insert can create duplicate jobs:

```mermaid
sequenceDiagram
    autonumber
    participant A as Request A
    participant B as Request B
    participant R as Dedup HashMap

    A->>R: check key
    R-->>A: missing
    B->>R: check same key
    R-->>B: missing
    A->>A: create job J1
    B->>B: create job J2
    A->>R: insert J1
    B->>R: insert J2
```

The planned fix is to perform lookup and insertion inside one mutex-protected critical section:

```mermaid
flowchart TD
    L["lock registry"] --> E{"dedup.entry(key)"}
    E -->|"occupied"| O["return existing job_id"]
    E -->|"vacant"| N["create job_id"]
    N --> D["insert dedup entry"]
    D --> J["insert job metadata"]
    J --> Q["enqueue canonical job"]
    O --> U["unlock registry"]
    Q --> U
```

The mutex protects registry mutation only. Upload streaming, hashing, FFmpeg execution, and artifact download happen outside the lock.

## Request Flow

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as API
    participant R as Registry
    participant Q as Queue
    participant W as Worker

    C->>API: POST /api/jobs
    API->>API: stream file + compute SHA-256
    API->>R: lock + get_or_create(DedupKey)
    alt new key
        R->>Q: enqueue job
        R-->>API: deduplicated=false
    else existing key
        R-->>API: deduplicated=true
    end
    API-->>C: job response
    Q->>W: dequeue job
    W->>W: run FFmpeg profile
```

If an upload is interrupted, the hash is not finalized and no job is registered.

## Job States

```mermaid
stateDiagram-v2
    [*] --> queued
    queued --> running
    running --> succeeded
    running --> failed
    succeeded --> [*]
    failed --> [*]
```

Invalid transitions are rejected.

## API Surface

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/health` | Health check |
| `POST` | `/api/jobs` | Upload video and create or reuse a job |
| `GET` | `/api/jobs` | List recent jobs |
| `GET` | `/api/jobs/{job_id}` | Read job status |
| `GET` | `/api/jobs/{job_id}/artifact` | Download succeeded artifact |

## Scope

| In scope | Out of scope |
|---|---|
| Rust/Tokio + Axum API | database persistence |
| multipart uploads | authentication |
| SHA-256 content hashing | session management |
| in-memory dedup registry | web dashboard |
| worker queue | distributed processing |
| YAML FFmpeg profiles | resumable upload |
| local filesystem artifacts | runtime profile reload |

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [API specification](docs/API_SPEC.md)
- [Test plan](docs/TEST_PLAN.md)
