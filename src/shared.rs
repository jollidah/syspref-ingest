use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum JobStatus {
    #[default]
    Queued,
    Running,
    Succeeded,
    Failed,
}


#[derive(Debug, Error)]
pub enum StatusTransitionError {
    #[error("Invalid transition from {0:?} to {1:?}")]
    InvalidTransition(JobStatus, JobStatus),
}

impl JobStatus {
    pub fn can_transition_to(&self, target: JobStatus) -> Result<(), StatusTransitionError> {
        match (self, target) {
            (JobStatus::Queued, JobStatus::Running) => Ok(()),
            (JobStatus::Running, JobStatus::Succeeded) => Ok(()),
            (JobStatus::Running, JobStatus::Failed) => Ok(()),
            _ => Err(StatusTransitionError::InvalidTransition(*self, target)),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, JobStatus::Succeeded | JobStatus::Failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_status_transition() {
        // Test all invalid transitions
        let invalid_transitions = vec![
            (JobStatus::Queued, JobStatus::Queued),     // queued -> queued
            (JobStatus::Queued, JobStatus::Succeeded),  // queued -> succeeded
            (JobStatus::Queued, JobStatus::Failed),     // queued -> failed
            (JobStatus::Running, JobStatus::Running),   // running -> running
            (JobStatus::Succeeded, JobStatus::Running), // succeeded -> running
            (JobStatus::Failed, JobStatus::Running),    // failed -> running
        ];

        for (from, to) in invalid_transitions {
            let result = from.can_transition_to(to);
            assert!(
                result.is_err(),
                "Transition {:?} -> {:?} should be invalid",
                from,
                to
            );
        }
    }

    #[test]
    fn valid_status_transitions() {
        // Test all valid transitions
        let valid_transitions = vec![
            (JobStatus::Queued, JobStatus::Running),
            (JobStatus::Running, JobStatus::Succeeded),
            (JobStatus::Running, JobStatus::Failed),
        ];

        for (from, to) in valid_transitions {
            let result = from.can_transition_to(to);
            assert!(
                result.is_ok(),
                "Transition {:?} -> {:?} should be valid",
                from,
                to
            );
        }
    }

    #[test]
    fn job_start_succeed_flow() {
        let mut job = Job::new(
            Uuid::new_v4(),
            "profile1".to_string(),
            "hash123".to_string(),
        );

        // Start the job
        assert!(job.start().is_ok());
        assert_eq!(job.status, JobStatus::Running);
        assert!(job.started_at.is_some());

        // Succeed the job
        let artifact_path = PathBuf::from("/tmp/output.mp4");
        assert!(job.succeed(artifact_path).is_ok());
        assert_eq!(job.status, JobStatus::Succeeded);
        assert!(job.finished_at.is_some());
        assert!(job.artifact.is_some());
    }

    #[test]
    fn job_start_fail_flow() {
        let mut job = Job::new(
            Uuid::new_v4(),
            "profile1".to_string(),
            "hash123".to_string(),
        );

        // Start the job
        assert!(job.start().is_ok());
        assert_eq!(job.status, JobStatus::Running);

        // Fail the job
        let error_msg = "FFmpeg crashed";
        assert!(job.fail(error_msg.to_string()).is_ok());
        assert_eq!(job.status, JobStatus::Failed);
        assert!(job.finished_at.is_some());
        assert_eq!(job.error, Some(error_msg.to_string()));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DedupKey {
    pub content_hash: String,
    pub profile: String,
}

impl DedupKey {
    pub fn new(content_hash: String, profile: String) -> Self {
        DedupKey {
            content_hash,
            profile,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub profile: String,
    pub content_hash: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub artifact: Option<Artifact>,
    pub error: Option<String>,
}

impl Job {
    pub fn new(job_id: Uuid, profile: String, content_hash: String) -> Self {
        Job {
            job_id,
            status: JobStatus::Queued,
            profile,
            content_hash,
            created_at: chrono::Utc::now(),
            started_at: None,
            finished_at: None,
            artifact: None,
            error: None,
        }
    }

    pub fn start(&mut self) -> Result<(), StatusTransitionError> {
        self.status.can_transition_to(JobStatus::Running)?;
        self.status = JobStatus::Running;
        self.started_at = Some(chrono::Utc::now());
        Ok(())
    }

    pub fn succeed(&mut self, artifact_path: PathBuf) -> Result<(), StatusTransitionError> {
        self.status.can_transition_to(JobStatus::Succeeded)?;
        self.status = JobStatus::Succeeded;
        self.finished_at = Some(chrono::Utc::now());
        self.artifact = Some(Artifact {
            path: artifact_path,
        });
        Ok(())
    }

    pub fn fail(&mut self, error: String) -> Result<(), StatusTransitionError> {
        self.status.can_transition_to(JobStatus::Failed)?;
        self.status = JobStatus::Failed;
        self.finished_at = Some(chrono::Utc::now());
        self.error = Some(error);
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Job not found: {0}")]
    JobNotFound(Uuid),
    #[error("Invalid status transition")]
    InvalidStatusTransition,
    #[error("FFmpeg error: {0}")]
    FfmpegError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("YAML error: {0}")]
    YamlError(#[from] serde_yaml::Error),
    #[error("Storage error: {0}")]
    StorageError(String),
}

pub type AppResult<T> = std::result::Result<T, AppError>;
