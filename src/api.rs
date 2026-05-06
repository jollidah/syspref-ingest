use crate::dedup::RegistryArc;
use crate::ffmpeg::Transcoder;
use crate::queue::JobSender;
use crate::shared::{AppError, DedupKey, Job, JobStatus};
use crate::storage::Storage;
use axum::{
    body::{Body, Bytes},
    extract::multipart::{Multipart, MultipartError, MultipartRejection},
    extract::{DefaultBodyLimit, Path, State},
    http::{header, StatusCode},
    response::{Json, Response},
    routing::{get, post},
    Router,
};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

/// Map a `MultipartError` raised during field iteration to the corresponding
/// `AppError`. `DefaultBodyLimit` overages surface here (status 413), while
/// any other parse failure (truncated body, header errors, etc.) is treated
/// as a wire-protocol violation.
fn map_multipart_err(err: MultipartError) -> AppError {
    if err.status() == StatusCode::PAYLOAD_TOO_LARGE {
        AppError::PayloadTooLarge
    } else {
        AppError::InvalidMultipart
    }
}

/// RAII guard: always removes the tracked tmp path on Drop. `remove_file`
/// errors (ENOENT after `fs::rename`, after explicit `cleanup_tmp`, or when
/// `File::create` failed before any bytes were written) are absorbed.
/// Satisfies docs/ARCHITECTURE.md "Upload connection dropped mid-stream →
/// Temp file is discarded".
struct TmpFileGuard(PathBuf);

impl Drop for TmpFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Clone)]
pub struct AppState {
    pub registry: RegistryArc,
    pub storage: Arc<Storage>,
    pub queue_tx: JobSender,
    pub transcoder: Arc<dyn Transcoder>,
}

fn ext_to_mime(ext: &str) -> &'static str {
    match ext {
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        _ => "application/octet-stream",
    }
}

fn status_str(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Succeeded => "succeeded",
        JobStatus::Failed => "failed",
    }
}

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn create_job(
    State(state): State<AppState>,
    multipart: Result<Multipart, MultipartRejection>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    // Extractor-level rejection (missing/invalid boundary, non-multipart
    // `Content-Type`, etc.) maps to `invalid_multipart`. axum 0.7 surfaces
    // `DefaultBodyLimit` overages later, via `MultipartError::status() == 413`
    // inside the field iteration loop, not here.
    let mut multipart = multipart.map_err(|_| AppError::InvalidMultipart)?;

    let mut file_data: Option<(String, String)> = None;
    let mut profile_str: Option<String> = None;
    // Request-scoped guard: armed when the file branch starts writing tmp,
    // disarmed only after the file is committed (renamed to canonical on
    // dedup-miss or explicit cleanup_tmp on dedup-hit). Any early return,
    // panic, or future cancellation between arm and commit triggers Drop,
    // which sync-removes the tmp file.
    let mut tmp_guard: Option<TmpFileGuard> = None;

    while let Some(mut field) = multipart.next_field().await.map_err(map_multipart_err)? {
        let name = field.name().unwrap_or("").to_string();

        if name == "file" {
            // docs/API_SPEC.md: "The server streams the file to a temporary
            // path. The server computes SHA-256 while reading the stream."
            // `Option::replace` returns the previous guard, which is dropped
            // here, cleaning up the earlier tmp if a duplicate `file` field
            // arrives.
            let filename = format!("upload_{}.tmp", uuid::Uuid::new_v4());
            let tmp_path = state.storage.tmp_path(&filename);
            tmp_guard.replace(TmpFileGuard(tmp_path.clone()));

            let mut hasher = Sha256::new();
            {
                let mut file = tokio::fs::File::create(&tmp_path).await?;
                while let Some(chunk) = field.chunk().await.map_err(map_multipart_err)? {
                    hasher.update(&chunk);
                    file.write_all(&chunk).await?;
                }
                file.flush().await?;
            }
            let hash = hex::encode(hasher.finalize());
            file_data = Some((hash, filename));
        } else if name == "profile" {
            let bytes: Bytes = field.bytes().await.map_err(map_multipart_err)?;
            let p = String::from_utf8(bytes.to_vec()).map_err(|_| AppError::InvalidMultipart)?;
            // Option A (plan §4-A T4 / D6): validate at parse time so a
            // profile-first request with an invalid name skips file IO.
            if state.transcoder.get_profile(&p).is_none() {
                return Err(AppError::UnknownProfile(p));
            }
            profile_str = Some(p);
        }
        // Unknown field names: drop the `Field` without reading. multer
        // skips its remaining bytes when `next_field()` is called next.
    }

    let (content_hash, filename) = file_data.ok_or(AppError::MissingFile)?;
    // Profile was already validated at parse time (Option A); on missing,
    // the guard cleans up the staged tmp via Drop on early return.
    let profile = profile_str.ok_or(AppError::MissingProfile)?;

    let key = DedupKey::new(content_hash.clone(), profile.clone());
    let profile_for_response = profile.clone();

    // Atomic dedup lookup/insert. Sync body — no `.await` while holding the
    // registry guard (docs/ARCHITECTURE.md, docs/TEST_PLAN.md).
    let (job_id, deduplicated, dedup_status) = {
        let mut registry = state.registry.lock().await;
        registry.get_or_create(key.clone(), profile)
    };

    if deduplicated {
        // Existing canonical job: discard tmp, do not enqueue. Surface
        // cleanup errors here; the guard's Drop on function return is a
        // best-effort fallback (ENOENT-safe).
        if let Err(e) = state.storage.cleanup_tmp(&filename) {
            return Err(AppError::StorageError(format!("cleanup_tmp: {}", e)));
        }
    } else {
        // New canonical job: stage tmp -> canonical input, then enqueue.
        // Order: create_job_dirs -> rename -> send. If any step fails after
        // the registry insert, roll back so future requests for the same key
        // can produce a fresh canonical job.
        let canonical_path = state.storage.job_input_path(&job_id).join("input.bin");
        let tmp_path = state.storage.tmp_path(&filename);

        let staging: Result<(), String> = async {
            state
                .storage
                .create_job_dirs(&job_id)
                .map_err(|e| format!("create_job_dirs: {}", e))?;
            fs::rename(&tmp_path, &canonical_path)
                .await
                .map_err(|e| format!("rename: {}", e))?;
            state
                .queue_tx
                .send(job_id)
                .map_err(|e| format!("queue send: {}", e))?;
            Ok(())
        }
        .await;

        if let Err(e) = staging {
            {
                let mut registry = state.registry.lock().await;
                registry.remove(&key, &job_id);
            }
            // Best-effort cleanup; guard's Drop is also ENOENT-safe if
            // rename already moved the tmp.
            let _ = state.storage.cleanup_tmp(&filename);
            return Err(AppError::StorageError(format!("staging failed: {}", e)));
        }
        // Staging succeeded: rename moved tmp to canonical. The guard's
        // Drop on function return will see ENOENT and absorb it.
    }

    // Spec: 201 for new canonical job, 200 for dedup hit. `dedup_status` is
    // already correct in both cases — `JobStatus::Queued` on a fresh insert,
    // the existing job's current state on a hit.
    let status_code = if deduplicated {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };

    let body = serde_json::json!({
        "job_id": job_id.to_string(),
        "status": status_str(dedup_status),
        "deduplicated": deduplicated,
        "profile": profile_for_response,
        "content_hash": content_hash,
    });

    Ok((status_code, Json(body)))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    // Snapshot under lock, drop guard, then serialize
    // (docs/ARCHITECTURE.md: "serialize responses after dropping the lock").
    let snapshot = {
        let registry = state.registry.lock().await;
        registry.get_job(&job_id).cloned()
    };
    let job = snapshot.ok_or(AppError::JobNotFound(job_id))?;

    Ok(Json(serde_json::json!({
        "job_id": job.job_id.to_string(),
        "status": status_str(job.status),
        "profile": job.profile,
        "content_hash": job.content_hash,
        "created_at": job.created_at.to_rfc3339(),
        "started_at": job.started_at.map(|t| t.to_rfc3339()),
        "finished_at": job.finished_at.map(|t| t.to_rfc3339()),
        "artifact": job.artifact.as_ref().map(|a| serde_json::json!({
            "path": a.path.display().to_string()
        })),
        "error": job.error,
    })))
}

pub async fn list_jobs(State(state): State<AppState>) -> Json<serde_json::Value> {
    // docs/ARCHITECTURE.md: snapshot under lock, then serialize after lock drops.
    let jobs: Vec<Job> = {
        let registry = state.registry.lock().await;
        registry.get_jobs().into_iter().cloned().collect()
    };

    let job_list: Vec<serde_json::Value> = jobs
        .iter()
        .map(|j| {
            serde_json::json!({
                "job_id": j.job_id.to_string(),
                "status": status_str(j.status),
                "profile": j.profile,
                "content_hash": j.content_hash,
            })
        })
        .collect();

    Json(serde_json::json!({ "jobs": job_list }))
}

pub async fn get_artifact(
    State(state): State<AppState>,
    Path(job_id): Path<uuid::Uuid>,
) -> Result<Response, AppError> {
    // docs/ARCHITECTURE.md: snapshot under lock, drop guard before any .await.
    let snapshot = {
        let registry = state.registry.lock().await;
        registry.get_job(&job_id).cloned()
    };
    let job = snapshot.ok_or(AppError::JobNotFound(job_id))?;

    match job.status {
        JobStatus::Queued | JobStatus::Running => Err(AppError::ArtifactNotReady),
        JobStatus::Failed => Err(AppError::JobFailed),
        JobStatus::Succeeded => {
            // Normal flow always sets artifact when transitioning to Succeeded;
            // surface a stable error if the invariant is violated.
            let path = job
                .artifact
                .ok_or_else(|| {
                    AppError::FfmpegError(format!("artifact missing for job '{}'", job.job_id))
                })?
                .path;
            let profile = state.transcoder.get_profile(&job.profile).ok_or_else(|| {
                AppError::FfmpegError(format!("profile '{}' missing at runtime", job.profile))
            })?;
            let ext = profile.output_extension.clone();
            let mime = ext_to_mime(&ext);

            // Stream the artifact to avoid loading large files into memory.
            // Body::from_stream does not set Content-Length; do it explicitly.
            let len = fs::metadata(&path).await?.len();
            let file = fs::File::open(&path).await?;
            let body = Body::from_stream(ReaderStream::new(file));

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                .header(
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}.{}\"", job_id, ext),
                )
                .header(header::CONTENT_LENGTH, len.to_string())
                .body(body)
                .map_err(|e| AppError::FfmpegError(format!("response build: {}", e)))
        }
    }
}

pub fn create_router(
    registry: RegistryArc,
    storage: Arc<Storage>,
    queue_tx: JobSender,
    transcoder: Arc<dyn Transcoder>,
    max_upload_bytes: usize,
) -> Router {
    let state = AppState {
        registry,
        storage,
        queue_tx,
        transcoder,
    };

    // axum 0.7 + matchit 0.7 use ":name" path-param syntax; "{name}" is matchit 0.8 / axum 0.8.
    // The body-limit layer is attached only to POST /api/jobs because the
    // GET endpoints carry no request body. axum 0.7 surfaces an overage as
    // `MultipartError::status() == 413`, which `map_multipart_err` maps to
    // `AppError::PayloadTooLarge`.
    Router::new()
        .route("/api/health", get(health))
        .route(
            "/api/jobs",
            post(create_job).layer(DefaultBodyLimit::max(max_upload_bytes)),
        )
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/:job_id", get(get_job))
        .route("/api/jobs/:job_id/artifact", get(get_artifact))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::create_registry;
    use crate::ffmpeg::{FfmpegProfile, FfmpegRunner};
    use crate::queue::{create_queue, JobReceiver};
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::response::IntoResponse;
    use tower::ServiceExt;

    // ---------- helpers ----------

    fn test_transcoder() -> Arc<dyn Transcoder> {
        Arc::new(FfmpegRunner::new(vec![FfmpegProfile {
            name: "p".to_string(),
            args: vec![],
            output_extension: "mp4".to_string(),
        }]))
    }

    /// Default test router with a generous body limit (1 MiB) so existing
    /// happy-path / dedup / artifact tests are unaffected by the upload cap.
    /// Body-limit-specific tests build their own router via `router_with_limit`.
    fn router() -> (Router, RegistryArc, Arc<Storage>, JobReceiver) {
        router_with_limit(1024 * 1024)
    }

    fn router_with_limit(
        max_upload_bytes: usize,
    ) -> (Router, RegistryArc, Arc<Storage>, JobReceiver) {
        let registry = create_registry();
        let tmp = std::env::temp_dir().join(format!("syspref_test_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(Storage::new(&tmp));
        storage.ensure_exists().unwrap();
        let (queue_tx, queue_rx) = create_queue();
        let app = create_router(
            registry.clone(),
            storage.clone(),
            queue_tx,
            test_transcoder(),
            max_upload_bytes,
        );
        // Return queue_rx so it stays alive in the test scope; otherwise
        // queue_tx.send() in create_job's staging fails with channel closed.
        (app, registry, storage, queue_rx)
    }

    fn multipart_body(boundary: &str, file_bytes: &[u8], profile: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"v.mp4\"\r\n\r\n",
        );
        body.extend_from_slice(file_bytes);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n\r\n");
        body.extend_from_slice(profile.as_bytes());
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    fn multipart_only_profile(boundary: &str, profile: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n\r\n");
        body.extend_from_slice(profile.as_bytes());
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    fn multipart_only_file(boundary: &str, file_bytes: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"v.mp4\"\r\n\r\n",
        );
        body.extend_from_slice(file_bytes);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    async fn read_json(resp: axum::response::Response) -> (StatusCode, serde_json::Value) {
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            panic!(
                "expected JSON body but got status {} body {:?}: {}",
                status,
                String::from_utf8_lossy(&bytes),
                e
            )
        });
        (status, v)
    }

    // ---------- ext_to_mime (PR #18 regression) ----------

    #[test]
    fn ext_to_mime_covers_known_and_unmapped_branches() {
        assert_eq!(ext_to_mime("mp4"), "video/mp4");
        assert_eq!(ext_to_mime("webm"), "video/webm");
        assert_eq!(ext_to_mime("mp3"), "audio/mpeg");
        assert_eq!(ext_to_mime("mov"), "application/octet-stream");
    }

    // ---------- direct-handler artifact tests (PR #18 / #4 regression) ----------

    fn make_state_with_succeeded_job(
        ext: &str,
        profile_name: &str,
        artifact_bytes: &[u8],
    ) -> (AppState, uuid::Uuid, tempfile::TempDir, JobReceiver) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let artifact_path = tmp.path().join(format!("proxy.{}", ext));
        std::fs::write(&artifact_path, artifact_bytes).expect("write artifact");

        let registry = create_registry();
        let job_id = uuid::Uuid::new_v4();
        {
            let mut reg = registry.try_lock().expect("uncontended");
            let mut job = Job::new(job_id, profile_name.to_string(), "deadbeef".to_string());
            job.start().expect("start");
            job.succeed(artifact_path.clone()).expect("succeed");
            reg.jobs.insert(job_id, job);
        }

        let storage = Arc::new(Storage::new(tmp.path()));
        let transcoder: Arc<dyn Transcoder> = Arc::new(FfmpegRunner::new(vec![FfmpegProfile {
            name: profile_name.to_string(),
            args: vec![],
            output_extension: ext.to_string(),
        }]));
        let (queue_tx, queue_rx) = create_queue();
        let state = AppState {
            registry,
            storage,
            queue_tx,
            transcoder,
        };
        (state, job_id, tmp, queue_rx)
    }

    fn header_str<'a>(resp: &'a axum::response::Response, name: &str) -> &'a str {
        resp.headers()
            .get(name)
            .expect("header present")
            .to_str()
            .expect("ascii")
    }

    #[tokio::test]
    async fn artifact_headers_derive_from_output_extension() {
        let cases: &[(&str, &str, &str, &[u8])] = &[
            ("mp4", "web_720p", "video/mp4", b"\0\0\0 ftypmp42"),
            ("webm", "web_720p_webm", "video/webm", b"\x1aE\xdf\xa3"),
        ];
        for (ext, profile_name, mime, bytes) in cases {
            let (state, job_id, _tmp, _rx) =
                make_state_with_succeeded_job(ext, profile_name, bytes);
            let resp = get_artifact(State(state), Path(job_id)).await.expect("ok");
            assert_eq!(header_str(&resp, "Content-Type"), *mime);
            assert_eq!(
                header_str(&resp, "Content-Disposition"),
                format!("attachment; filename=\"{}.{}\"", job_id, ext)
            );
        }
    }

    /// When profile lookup fails the handler must still return 500, but the
    /// envelope must not leak the profile name or any other internal detail.
    #[tokio::test]
    async fn artifact_500_envelope_hides_profile_lookup_detail() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let artifact_path = tmp.path().join("proxy.mp4");
        std::fs::write(&artifact_path, b"x").expect("write");

        let registry = create_registry();
        let job_id = uuid::Uuid::new_v4();
        {
            let mut reg = registry.try_lock().expect("uncontended");
            let mut job = Job::new(job_id, "vanished_profile".to_string(), "h".to_string());
            job.start().expect("start");
            job.succeed(artifact_path).expect("succeed");
            reg.jobs.insert(job_id, job);
        }
        let storage = Arc::new(Storage::new(tmp.path()));
        let transcoder: Arc<dyn Transcoder> = Arc::new(FfmpegRunner::new(vec![FfmpegProfile {
            name: "different_profile".to_string(),
            args: vec![],
            output_extension: "mp4".to_string(),
        }]));
        let (queue_tx, _queue_rx) = create_queue();
        let state = AppState {
            registry,
            storage,
            queue_tx,
            transcoder,
        };

        let err = get_artifact(State(state), Path(job_id))
            .await
            .expect_err("err");
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "internal_error");
        assert_eq!(body["error"]["message"], "internal server error");
        assert!(!body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("vanished_profile"));
    }

    // ---------- HTTP-level acceptance for issue #5 ----------

    #[tokio::test]
    async fn post_new_job_returns_201_and_queued() {
        let (app, _registry, _storage, _queue_rx) = router();
        let boundary = "BNDRY";
        let body = multipart_body(boundary, b"hello world", "p");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["deduplicated"], serde_json::json!(false));
        assert_eq!(body["status"], "queued");
        assert_eq!(body["profile"], "p");
        assert!(body["job_id"].as_str().is_some());
        // Issue #10 regression: content_hash matches the known SHA-256 of
        // "hello world" (b94d27b9...). Asserting the exact hex catches any
        // chunk-loop bug that the previous `is_some()` check would have
        // masked.
        assert_eq!(
            body["content_hash"],
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    /// Issue #10: streamed SHA-256 of an empty file equals the well-known
    /// digest of zero bytes. Proves the chunk loop produces a valid hash
    /// even when `field.chunk()` yields no data.
    #[tokio::test]
    async fn post_empty_file_yields_known_empty_sha256() {
        let (app, _registry, _storage, _queue_rx) = router();
        let boundary = "BNDRY";
        let body = multipart_body(boundary, b"", "p");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            body["content_hash"],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// Issue #10: chunked hashing produces the same digest as one-shot
    /// hashing on a payload large enough to typically span multiple
    /// `field.chunk()` calls. Uses a generous body limit so the streaming
    /// path is exercised end-to-end.
    #[tokio::test]
    async fn streamed_hash_matches_one_shot_for_multi_chunk_payload() {
        let (app, _registry, _storage, _queue_rx) = router_with_limit(8 * 1024 * 1024);
        // Deterministic, non-trivial 256 KiB payload.
        let payload: Vec<u8> = (0..(256 * 1024) as u32).map(|i| (i % 251) as u8).collect();
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let expected = hex::encode(hasher.finalize());

        let boundary = "BNDRY";
        let body = multipart_body(boundary, &payload, "p");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["content_hash"], expected);
    }

    /// Issue #10 / plan §6 E4: when the request body errors mid-stream after
    /// the file branch has started writing, the handler must return without
    /// leaving a partial tmp on disk and without inserting any registry or
    /// queue state. Exercises `TmpFileGuard`'s Drop on the early-return path.
    #[tokio::test]
    async fn mid_stream_body_error_clears_tmp_and_state() {
        use futures::stream;
        let (app, registry, storage, queue_rx) = router();
        let boundary = "BNDRY";
        // Multipart prologue + file headers + a few bytes of body, then an
        // unrecoverable stream error. The multipart parser surfaces this as
        // a `MultipartError`, which `map_multipart_err` maps to
        // `AppError::InvalidMultipart`.
        let prologue = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"v.mp4\"\r\n\r\n"
        );
        let chunks: Vec<Result<axum::body::Bytes, std::io::Error>> = vec![
            Ok(axum::body::Bytes::from(prologue.into_bytes())),
            Ok(axum::body::Bytes::from_static(b"hello mid-stream")),
            Err(std::io::Error::other("simulated client disconnect")),
        ];
        let body = axum::body::Body::from_stream(stream::iter(chunks));
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, json) = read_json(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["error"]["code"], "invalid_multipart");

        // Registry and queue must be untouched.
        let registry_state = registry.lock().await;
        assert!(registry_state.dedup.is_empty());
        assert!(registry_state.jobs.is_empty());
        drop(registry_state);
        assert!(queue_rx.lock().await.try_recv().is_err());

        // tmp dir must hold no partial upload file (guard's Drop ran).
        let tmp_dir = storage.tmp_path("");
        let tmp_dir = tmp_dir.parent().unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(tmp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("upload_"))
            .collect();
        assert!(leftovers.is_empty(), "tmp leftovers: {:?}", leftovers);
    }

    /// Issue #10 / plan §6 E12a: with Option A profile validation, an unknown
    /// `profile` field arriving BEFORE the `file` field short-circuits before
    /// any file IO happens. Asserts the response envelope, empty registry/
    /// queue, AND the absence of any `upload_*.tmp` file (the file branch
    /// must not have run).
    #[tokio::test]
    async fn unknown_profile_first_skips_file_io() {
        let (app, registry, storage, queue_rx) = router();
        let boundary = "BNDRY";
        // profile FIRST (so Option A's parse-time validation fires before
        // the file branch is reached), then file.
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n\r\n");
        body.extend_from_slice(b"does_not_exist");
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"v.mp4\"\r\n\r\n",
        );
        body.extend_from_slice(b"would-be file content");
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, json) = read_json(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["error"]["code"], "unknown_profile");

        let registry_state = registry.lock().await;
        assert!(registry_state.dedup.is_empty());
        assert!(registry_state.jobs.is_empty());
        drop(registry_state);
        assert!(queue_rx.lock().await.try_recv().is_err());

        // tmp dir must be clean — proves the file branch never ran (no IO
        // wasted on staging the file before the unknown_profile check).
        let tmp_dir = storage.tmp_path("");
        let tmp_dir = tmp_dir.parent().unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(tmp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("upload_"))
            .collect();
        assert!(leftovers.is_empty(), "tmp leftovers: {:?}", leftovers);
    }

    /// Issue #10 / plan §6 E11: a duplicate `file` field is processed with
    /// last-wins semantics. The first tmp must be cleaned up by the prior
    /// guard's Drop, the canonical hash/content come from the LAST file
    /// field, and only one canonical input is committed.
    #[tokio::test]
    async fn duplicate_file_field_last_wins_and_clears_first_tmp() {
        let (app, _registry, storage, _queue_rx) = router();
        let boundary = "BNDRY";
        let first_payload = b"FIRST file content";
        let last_payload = b"LAST file content";
        let mut body = Vec::new();
        for payload in &[first_payload.as_slice(), last_payload.as_slice()] {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(
                b"Content-Disposition: form-data; name=\"file\"; filename=\"v.mp4\"\r\n\r\n",
            );
            body.extend_from_slice(payload);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n\r\n");
        body.extend_from_slice(b"p");
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, response_body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CREATED);

        // Last-wins: hash comes from the second (last) `file` field.
        let mut hasher = Sha256::new();
        hasher.update(last_payload);
        let expected = hex::encode(hasher.finalize());
        assert_eq!(response_body["content_hash"], expected);

        // Canonical input.bin contains the LAST payload (not the first).
        let job_id = uuid::Uuid::parse_str(response_body["job_id"].as_str().unwrap()).unwrap();
        let canonical = storage.job_input_path(&job_id).join("input.bin");
        let canonical_bytes = std::fs::read(&canonical).expect("canonical input present");
        assert_eq!(canonical_bytes.as_slice(), last_payload);

        // No `upload_*.tmp` left behind: the first tmp was cleaned by the
        // prior guard's Drop when the second `file` field arrived.
        let tmp_dir = storage.tmp_path("");
        let tmp_dir = tmp_dir.parent().unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(tmp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("upload_"))
            .collect();
        assert!(leftovers.is_empty(), "tmp leftovers: {:?}", leftovers);
    }

    /// Issue #10: a successful upload commits the canonical input under
    /// `jobs/<id>/input/input.bin` with byte-identical content and leaves
    /// no `upload_*.tmp` behind. Catches regressions where the streaming
    /// path forgets to rename or leaves the tmp.
    #[tokio::test]
    async fn streamed_upload_commits_canonical_and_clears_tmp() {
        let (app, _registry, storage, _queue_rx) = router();
        let payload = b"streaming committal check";
        let boundary = "BNDRY";
        let body = multipart_body(boundary, payload, "p");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CREATED);

        let job_id = uuid::Uuid::parse_str(body["job_id"].as_str().unwrap()).unwrap();
        let canonical = storage.job_input_path(&job_id).join("input.bin");
        let canonical_bytes = std::fs::read(&canonical).expect("canonical input present");
        assert_eq!(canonical_bytes.as_slice(), payload);

        let tmp_dir = storage.tmp_path("");
        let tmp_dir = tmp_dir.parent().unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(tmp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("upload_"))
            .collect();
        assert!(leftovers.is_empty(), "tmp leftovers: {:?}", leftovers);
    }

    #[tokio::test]
    async fn post_dedup_returns_200_and_current_status() {
        let (app, registry, _storage, _queue_rx) = router();
        let boundary = "BNDRY";
        let body = multipart_body(boundary, b"same content", "p");

        let make_req = || {
            Request::builder()
                .method("POST")
                .uri("/api/jobs")
                .header(
                    "Content-Type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(axum::body::Body::from(body.clone()))
                .unwrap()
        };

        let resp1 = app.clone().oneshot(make_req()).await.unwrap();
        let (status1, body1) = read_json(resp1).await;
        assert_eq!(status1, StatusCode::CREATED);
        let job_id = body1["job_id"].as_str().unwrap().to_string();

        // Manually transition the canonical job to Running so we can prove
        // the dedup response carries the *current* status (not "queued").
        {
            let id = uuid::Uuid::parse_str(&job_id).unwrap();
            let mut state = registry.lock().await;
            state.jobs.get_mut(&id).unwrap().start().unwrap();
        }

        let resp2 = app.oneshot(make_req()).await.unwrap();
        let (status2, body2) = read_json(resp2).await;
        assert_eq!(status2, StatusCode::OK);
        assert_eq!(body2["deduplicated"], serde_json::json!(true));
        assert_eq!(body2["status"], "running");
        assert_eq!(body2["job_id"], serde_json::json!(job_id));
    }

    /// `POST /api/jobs` with a missing required field returns 400 + the
    /// `{"error":{"code","message"}}` envelope. Verifies the strict body
    /// shape on the `missing_file` case so any envelope drift is caught.
    #[tokio::test]
    async fn post_missing_field_returns_400_envelope() {
        let cases: &[(&str, Vec<u8>, &str, &str)] = &[
            (
                "missing file",
                multipart_only_profile("BNDRY", "p"),
                "missing_file",
                "file field is missing",
            ),
            (
                "missing profile",
                multipart_only_file("BNDRY", b"hello"),
                "missing_profile",
                "profile field is missing",
            ),
        ];
        for (label, body, want_code, want_message) in cases {
            let (app, _registry, _storage, _queue_rx) = router();
            let req = Request::builder()
                .method("POST")
                .uri("/api/jobs")
                .header(
                    "Content-Type",
                    "multipart/form-data; boundary=BNDRY".to_string(),
                )
                .body(axum::body::Body::from(body.clone()))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            let (status, json) = read_json(resp).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "case={label}");
            assert_eq!(
                json,
                serde_json::json!({
                    "error": { "code": *want_code, "message": *want_message }
                }),
                "case={label}"
            );
        }
    }

    #[tokio::test]
    async fn get_unknown_job_returns_envelope() {
        let (app, _registry, _storage, _queue_rx) = router();
        let id = uuid::Uuid::new_v4();
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{id}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "unknown_job_id");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&id.to_string()));
    }

    #[tokio::test]
    async fn get_job_artifact_shape_object_when_succeeded() {
        let (app, registry, _storage, _queue_rx) = router();
        let job_id = {
            let mut state = registry.lock().await;
            let key = DedupKey::new("h".into(), "p".into());
            let (id, _, _) = state.get_or_create(key, "p".into());
            let job = state.jobs.get_mut(&id).unwrap();
            job.start().unwrap();
            job.succeed(std::path::PathBuf::from("/tmp/out.mp4"))
                .unwrap();
            id
        };

        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{job_id}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "succeeded");
        assert!(body["artifact"].is_object());
        assert_eq!(body["artifact"]["path"], "/tmp/out.mp4");
    }

    #[tokio::test]
    async fn get_job_artifact_null_when_not_succeeded() {
        let (app, registry, _storage, _queue_rx) = router();
        let job_id = {
            let mut state = registry.lock().await;
            let key = DedupKey::new("h2".into(), "p".into());
            let (id, _, _) = state.get_or_create(key, "p".into());
            id
        };

        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{job_id}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["artifact"], serde_json::Value::Null);
    }

    /// Covers every error envelope branch of `GET /api/jobs/:id/artifact`:
    /// queued and running both map to `artifact_not_ready`, failed maps to
    /// `job_failed`, and an unknown job id maps to `unknown_job_id`.
    #[tokio::test]
    async fn get_artifact_envelope_branches() {
        let (app, registry, _storage, _queue_rx) = router();

        let mk_get = |id: &str| {
            Request::builder()
                .method("GET")
                .uri(format!("/api/jobs/{id}/artifact"))
                .body(axum::body::Body::empty())
                .unwrap()
        };

        let (queued_id, running_id, failed_id) = {
            let mut state = registry.lock().await;
            let qid = state
                .get_or_create(DedupKey::new("hq".into(), "p".into()), "p".into())
                .0;
            let rid = state
                .get_or_create(DedupKey::new("hr".into(), "p".into()), "p".into())
                .0;
            state.jobs.get_mut(&rid).unwrap().start().unwrap();
            let fid = state
                .get_or_create(DedupKey::new("hf".into(), "p".into()), "p".into())
                .0;
            let fjob = state.jobs.get_mut(&fid).unwrap();
            fjob.start().unwrap();
            fjob.fail("ffmpeg crashed".into()).unwrap();
            (qid, rid, fid)
        };
        let unknown_id = uuid::Uuid::new_v4();

        let cases = [
            (
                queued_id.to_string(),
                StatusCode::CONFLICT,
                "artifact_not_ready",
            ),
            (
                running_id.to_string(),
                StatusCode::CONFLICT,
                "artifact_not_ready",
            ),
            (failed_id.to_string(), StatusCode::CONFLICT, "job_failed"),
            (
                unknown_id.to_string(),
                StatusCode::NOT_FOUND,
                "unknown_job_id",
            ),
        ];

        for (id, want_status, want_code) in cases {
            let resp = app.clone().oneshot(mk_get(&id)).await.unwrap();
            let (status, body) = read_json(resp).await;
            assert_eq!(status, want_status, "id={id}");
            assert_eq!(body["error"]["code"], want_code, "id={id}");
        }
    }

    // ---------- HTTP-level acceptance for issue #6 (request validation) ----------

    /// E3: unregistered profile name is rejected before staging/enqueue with
    /// the documented 400 + `unknown_profile` envelope. Side-assertion: the
    /// tmp directory is empty afterwards (cleanup wrapper) and no job /
    /// dedup entry was created in the registry, no enqueue happened.
    #[tokio::test]
    async fn post_unknown_profile_returns_400_envelope_and_cleans_up() {
        let (app, registry, storage, queue_rx) = router();
        let boundary = "BNDRY";
        let body = multipart_body(boundary, b"hello world", "does_not_exist");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, json) = read_json(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["error"]["code"], "unknown_profile");
        // Display intentionally hides the offending profile name.
        assert_eq!(json["error"]["message"], "profile does not exist");
        assert!(!json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does_not_exist"));

        // Side assertions: registry and queue are untouched.
        let registry_state = registry.lock().await;
        assert!(registry_state.dedup.is_empty(), "dedup map must be empty");
        assert!(registry_state.jobs.is_empty(), "jobs map must be empty");
        drop(registry_state);
        assert!(
            queue_rx.lock().await.try_recv().is_err(),
            "queue must be empty"
        );

        // Side assertion: no leftover tmp file. `tmp_path("")` returns the
        // tmp directory itself (base_path/tmp), which Storage::ensure_exists
        // creates upfront, so it should exist but be empty after cleanup.
        let tmp_dir = storage.tmp_path("");
        let entries: Vec<_> = std::fs::read_dir(&tmp_dir)
            .unwrap_or_else(|e| panic!("read tmp dir {tmp_dir:?}: {e}"))
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert!(
            entries.is_empty(),
            "tmp dir must be empty, found: {entries:?}"
        );
    }

    /// E4 + L1: the configured body limit produces a 413 + `payload_too_large`
    /// JSON envelope (not the default plain-text rejection). Proves that
    /// `DefaultBodyLimit` overage flows through `MultipartError::status()`
    /// into the handler so envelope mapping kicks in.
    #[tokio::test]
    async fn post_oversized_body_returns_413_envelope() {
        // Tight cap so a small but normal-looking multipart body trips it.
        let (app, _registry, _storage, _queue_rx) = router_with_limit(256);
        let boundary = "BNDRY";
        // ~1 KiB file payload guarantees the encoded body exceeds 256 bytes.
        let payload = vec![b'A'; 1024];
        let body = multipart_body(boundary, &payload, "p");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, json) = read_json(resp).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(json["error"]["code"], "payload_too_large");
        assert_eq!(json["error"]["message"], "request body exceeds limit");
    }

    /// C5 / C10: malformed multipart at the wire-protocol layer maps to
    /// 400 + `invalid_multipart`. Covers the extractor-level rejection
    /// (non-multipart Content-Type).
    #[tokio::test]
    async fn post_non_multipart_content_type_returns_invalid_multipart() {
        let (app, _registry, _storage, _queue_rx) = router();
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(r#"{"profile":"p"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, json) = read_json(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["error"]["code"], "invalid_multipart");
        assert_eq!(json["error"]["message"], "invalid multipart request");
    }

    /// C5: invalid UTF-8 bytes in the `profile` field surface as
    /// `invalid_multipart`, not `missing_profile` — the multipart envelope is
    /// well-formed but the field's payload violates the textual contract.
    /// Side-assertion: when the invalid `profile` arrives AFTER a valid
    /// `file` part, the staged tmp must be cleaned up (this PR introduced
    /// the InvalidMultipart mapping and owns the cleanup).
    #[tokio::test]
    async fn post_invalid_utf8_profile_returns_invalid_multipart() {
        let (app, _registry, storage, _queue_rx) = router();
        let boundary = "BNDRY";
        // Build a multipart body where the `profile` part contains a lone
        // 0xFF byte (invalid UTF-8). `file` part is included first so the
        // tmp-cleanup side-assertion has something to verify.
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"v.mp4\"\r\n\r\n",
        );
        body.extend_from_slice(b"hello");
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n\r\n");
        body.extend_from_slice(&[0xFFu8]);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, json) = read_json(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["error"]["code"], "invalid_multipart");

        let tmp_dir = storage.tmp_path("");
        let entries: Vec<_> = std::fs::read_dir(&tmp_dir)
            .unwrap_or_else(|e| panic!("read tmp dir {tmp_dir:?}: {e}"))
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert!(
            entries.is_empty(),
            "tmp dir must be empty after invalid-utf8 profile, found: {entries:?}"
        );
    }
}
