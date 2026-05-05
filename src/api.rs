use crate::dedup::RegistryArc;
use crate::queue::JobSender;
use crate::shared::{DedupKey, Job};
use crate::storage::Storage;
use axum::{
    extract::multipart::Multipart,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::fs;

pub struct AppState {
    pub registry: RegistryArc,
    pub storage: Arc<Storage>,
    pub queue_tx: JobSender,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        AppState {
            registry: Arc::clone(&self.registry),
            storage: self.storage.clone(),
            queue_tx: self.queue_tx.clone(),
        }
    }
}

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn create_job(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut file_data = None;
    let mut profile_str: Option<String> = None;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        (StatusCode::BAD_REQUEST, format!("Failed to read field: {}", e))
    })? {
        let name = field.name().unwrap_or("").to_string();

        // Read the bytes using axum's Bytes
        use axum::body::Bytes;
        let bytes: Bytes = field.bytes().await.map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("Failed to read bytes: {}", e))
        })?;

        if name == "file" {
            // Compute SHA-256 while reading
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let hash = hex::encode(hasher.finalize());

            // Write to temp file
            let filename = format!("upload_{}.tmp", uuid::Uuid::new_v4());
            let tmp_path = state.storage.tmp_path(&filename);

            fs::write(&tmp_path, &bytes)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to write file: {}", e)))?;

            file_data = Some((hash, filename));
        } else if name == "profile" {
            profile_str = Some(String::from_utf8(bytes.to_vec()).map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("Invalid UTF-8 in profile: {}", e))
            })?);
        }
    }

    let (content_hash, filename) = file_data.ok_or((
        StatusCode::BAD_REQUEST,
        "Missing required field: file".to_string(),
    ))?;
    let profile = profile_str.ok_or((
        StatusCode::BAD_REQUEST,
        "Missing required field: profile".to_string(),
    ))?;

    // Create DedupKey
    let key = DedupKey::new(content_hash.clone(), profile.clone());

    // Atomic dedup lookup/insert. Synchronous body — no `.await` while holding
    // the registry guard (docs/ARCHITECTURE.md, docs/TEST_PLAN.md).
    let profile_for_response = profile.clone();
    let (job_id, deduplicated) = {
        let mut registry = state.registry.lock().await;
        registry.get_or_create(key.clone(), profile)
    };

    if deduplicated {
        // Existing canonical job: discard tmp, do not enqueue.
        state.storage.cleanup_tmp(&filename).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to cleanup tmp: {}", e),
            )
        })?;
    } else {
        // New canonical job: stage tmp -> canonical input, then enqueue.
        // Order: create_job_dirs -> rename -> send. If any step fails after
        // the registry insert, roll back the dedup/jobs entries so future
        // requests for the same key can produce a fresh canonical job
        // (docs/API_SPEC.md: "creates one canonical job and enqueues it").
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
            // Best-effort tmp cleanup. Ignore errors (tmp may already be
            // gone if rename succeeded before a later step failed).
            let _ = state.storage.cleanup_tmp(&filename);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("staging failed: {}", e),
            ));
        }
    }

    let response = serde_json::json!({
        "job_id": job_id.to_string(),
        "status": "queued",
        "deduplicated": deduplicated,
        "profile": profile_for_response,
        "content_hash": content_hash
    });

    Ok(Json(response))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let registry = state.registry.lock().await;
    let job = registry
        .get_job(&job_id)
        .ok_or((StatusCode::NOT_FOUND, format!("Job {} not found", job_id)))?;

    let response = serde_json::json!({
        "job_id": job.job_id.to_string(),
        "status": match job.status {
            crate::shared::JobStatus::Queued => "queued",
            crate::shared::JobStatus::Running => "running",
            crate::shared::JobStatus::Succeeded => "succeeded",
            crate::shared::JobStatus::Failed => "failed",
        },
        "profile": job.profile,
        "content_hash": job.content_hash,
        "created_at": job.created_at.to_rfc3339(),
        "started_at": job.started_at.map(|t| t.to_rfc3339()),
        "finished_at": job.finished_at.map(|t| t.to_rfc3339()),
        "artifact": job.artifact.as_ref().map(|a| a.path.display().to_string()),
        "error": job.error
    });

    Ok(Json(response))
}

pub async fn list_jobs(State(state): State<AppState>) -> Json<serde_json::Value> {
    let registry = state.registry.lock().await;
    let jobs: Vec<&Job> = registry.get_jobs();

    let job_list: Vec<serde_json::Value> = jobs
        .iter()
        .map(|j| serde_json::json!({
            "job_id": j.job_id.to_string(),
            "status": match j.status {
                crate::shared::JobStatus::Queued => "queued",
                crate::shared::JobStatus::Running => "running",
                crate::shared::JobStatus::Succeeded => "succeeded",
                crate::shared::JobStatus::Failed => "failed",
            },
            "profile": j.profile,
            "content_hash": j.content_hash
        }))
        .collect();

    Json(serde_json::json!({ "jobs": job_list }))
}

pub async fn get_artifact(
    State(state): State<AppState>,
    Path(job_id): Path<uuid::Uuid>,
) -> Result<axum::response::Response, (StatusCode, String)> {
    let registry = state.registry.lock().await;
    let job = registry
        .get_job(&job_id)
        .ok_or((StatusCode::NOT_FOUND, format!("Job {} not found", job_id)))?;

    match job.status {
        crate::shared::JobStatus::Succeeded => {
            if let Some(ref artifact) = job.artifact {
                match fs::read(&artifact.path).await {
                    Ok(bytes) => {
                        let mime_str = "video/mp4";
                        let mime: mime::Mime = mime_str.parse().unwrap_or(mime::APPLICATION_OCTET_STREAM);
                        Ok(axum::response::Response::builder()
                            .header("Content-Type", mime.to_string())
                            .body(bytes.into())
                            .unwrap())
                    }
                    Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read artifact: {}", e))),
                }
            } else {
                Err((StatusCode::NOT_FOUND, "Artifact not found".to_string()))
            }
        }
        crate::shared::JobStatus::Queued | crate::shared::JobStatus::Running => {
            Err((StatusCode::SERVICE_UNAVAILABLE, "Job not completed yet".to_string()))
        }
        crate::shared::JobStatus::Failed => {
            Err((StatusCode::BAD_GATEWAY, format!("Job failed: {:?}", job.error)))
        }
    }
}

pub fn create_router(
    registry: RegistryArc,
    storage: Arc<Storage>,
    queue_tx: JobSender,
) -> Router {
    let state = AppState {
        registry,
        storage,
        queue_tx,
    };

    Router::new()
        .route("/api/health", get(health))
        .route("/api/jobs", post(create_job))
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/{job_id}", get(get_job))
        .route("/api/jobs/{job_id}/artifact", get(get_artifact))
        .with_state(state)
}
