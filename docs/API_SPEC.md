# API Specification

## Base URL

```text
http://localhost:8080
```

This document describes the target API contract. Endpoints will be implemented in subsequent PRs.

---

## `GET /api/health`

### Response

```json
{
  "status": "ok"
}
```

---

## `POST /api/jobs`

Create or deduplicate a transcoding job.

### Request

```text
Content-Type: multipart/form-data
```

Fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `file` | file | yes | Uploaded video file |
| `profile` | string | yes | Profile name defined in YAML |

### Behavior

If `(content_hash, profile)` does not exist:

- create new canonical job
- enqueue job
- return `deduplicated=false`

If `(content_hash, profile)` already exists:

- do not create a new job
- do not enqueue
- return existing `job_id`
- return `deduplicated=true`

### Response: New Job

```json
{
  "job_id": "uuid",
  "status": "queued",
  "deduplicated": false,
  "profile": "web_720p",
  "content_hash": "sha256..."
}
```

### Response: Deduplicated Job

```json
{
  "job_id": "uuid",
  "status": "queued",
  "deduplicated": true,
  "profile": "web_720p",
  "content_hash": "sha256..."
}
```

---

## `GET /api/jobs/{job_id}`

### Response

```json
{
  "job_id": "uuid",
  "status": "succeeded",
  "profile": "web_720p",
  "content_hash": "sha256...",
  "created_at": "2026-04-27T00:00:00Z",
  "started_at": "2026-04-27T00:00:01Z",
  "finished_at": "2026-04-27T00:00:03Z",
  "artifact": {
    "path": "data/jobs/{job_id}/output/proxy.mp4"
  },
  "error": null
}
```

---

## `GET /api/jobs`

Returns recent jobs.

Planned response:

```json
{
  "jobs": [
    {
      "job_id": "uuid",
      "status": "succeeded",
      "profile": "web_720p",
      "content_hash": "sha256..."
    }
  ]
}
```

---

## `GET /api/jobs/{job_id}/artifact`

Downloads the output artifact.

Rules:

- `succeeded`: return artifact file
- `queued` / `running`: return not-ready error
- `failed`: return failure error
- unknown `job_id`: return 404
