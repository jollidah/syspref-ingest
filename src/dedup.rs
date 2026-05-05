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

impl Default for RegistryState {
    fn default() -> Self {
        Self::new()
    }
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
    pub async fn get_or_create(&mut self, key: DedupKey, profile: String) -> (Uuid, bool) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn dedup_registry_concurrent_access() {
        // Create shared registry and key
        let registry = create_registry();
        let key = DedupKey::new("same_hash".to_string(), "profile1".to_string());

        // Spawn 100 tasks that all try to get_or_create the same key
        let mut handles: Vec<JoinHandle<(Uuid, bool)>> = vec![];
        for _ in 0..100 {
            let reg = registry.clone();
            let key = key.clone();
            let handle = tokio::spawn(async move {
                let mut r = reg.lock().await;
                r.get_or_create(key, "profile1".to_string()).await
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete and collect results
        let results: Vec<(Uuid, bool)> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Extract the job IDs and dedup flags
        let job_ids: Vec<Uuid> = results.iter().map(|r| r.0).collect();
        let dedup_flags: Vec<bool> = results.iter().map(|r| r.1).collect();

        // All job IDs should be the same (one canonical job)
        let first_id = job_ids[0];
        for &id in &job_ids {
            assert_eq!(id, first_id, "All jobs should have the same ID");
        }

        // Exactly one should have dedup=false, rest should be true
        let false_count = dedup_flags.iter().filter(|&&x| !x).count();
        assert_eq!(
            false_count, 1,
            "Exactly one job should be newly created (dedup=false)"
        );

        // Verify registry state
        let r = registry.lock().await;
        assert_eq!(r.dedup.len(), 1, "Dedup map should have exactly one entry");
        assert_eq!(
            r.jobs.len(),
            1,
            "Jobs map should have exactly one canonical job"
        );
    }
}
