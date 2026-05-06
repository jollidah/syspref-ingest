use crate::shared::{DedupKey, Job};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct DedupEntry {
    pub job_id: Uuid,
}

pub struct RegistryState {
    pub dedup: HashMap<DedupKey, DedupEntry>,
    pub jobs: HashMap<Uuid, Job>,
}

impl RegistryState {
    pub fn new() -> Self {
        RegistryState {
            dedup: HashMap::new(),
            jobs: HashMap::new(),
        }
    }

    /// Atomic get-or-create operation under lock.
    /// Returns (job_id, was_deduplicated).
    pub fn get_or_create(
        &mut self,
        key: DedupKey,
        profile: String,
    ) -> (Uuid, bool) {
        let job_id = Uuid::new_v4();

        // Check if we have an existing job with same content_hash
        let existing_job_id = self.dedup.get(&key).map(|e| e.job_id);

        if let Some(existing_id) = existing_job_id {
            return (existing_id, true);
        }

        // Create new job entry
        let job = Job::new(job_id, profile.clone(), key.content_hash.clone());
        self.jobs.insert(job_id, job);
        self.dedup.insert(key, DedupEntry { job_id });

        (job_id, false)
    }

    /// Roll back a freshly created (key, job_id) when staging or enqueue fails
    /// before the job becomes effective. Idempotent. The dedup entry is only
    /// removed if it still points at the same job_id (another canonical job
    /// may have replaced it under contention; do not clobber that).
    pub fn remove(&mut self, key: &DedupKey, job_id: &Uuid) {
        if matches!(self.dedup.get(key), Some(entry) if entry.job_id == *job_id) {
            self.dedup.remove(key);
        }
        self.jobs.remove(job_id);
    }

    pub fn get_job(&self, job_id: &Uuid) -> Option<&Job> {
        self.jobs.get(job_id)
    }

    pub fn get_jobs(&self) -> Vec<&Job> {
        self.jobs.values().collect()
    }
}

pub type RegistryArc = Arc<Mutex<RegistryState>>;

pub fn create_registry() -> RegistryArc {
    Arc::new(Mutex::new(RegistryState::new()))
}
