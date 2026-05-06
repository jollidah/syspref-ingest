use crate::dedup::RegistryArc;
use crate::ffmpeg::FfmpegRunner;
use crate::queue::JobReceiver;
use crate::shared::AppResult;
use crate::storage::Storage;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct Worker {
    id: usize,
    registry: RegistryArc,
    queue_rx: JobReceiver,
    ffmpeg_runner: FfmpegRunner,
    storage: Arc<Storage>,
}

impl Worker {
    pub fn new(
        id: usize,
        registry: RegistryArc,
        queue_rx: JobReceiver,
        ffmpeg_runner: FfmpegRunner,
        storage: Arc<Storage>,
    ) -> Self {
        Worker {
            id,
            registry,
            queue_rx,
            ffmpeg_runner,
            storage,
        }
    }

    pub async fn run(&self) -> AppResult<()> {
        loop {
            // Receiver is shared across workers via Arc<Mutex<_>>; only one
            // worker holds the guard at a time, so each enqueued JobId is
            // delivered to at most one worker (docs/ARCHITECTURE.md Job Queue
            // Contract). Holding this mutex across `recv().await` is
            // intentional and distinct from the no-await-under-registry-lock
            // rule.
            let job_id = {
                let mut rx = self.queue_rx.lock().await;
                match rx.recv().await {
                    Some(id) => id,
                    None => break, // all senders dropped
                }
            };

            // Snapshot profile + flip to running under a narrow registry
            // guard. No `.await` inside this block.
            let profile_name = {
                let mut registry = self.registry.lock().await;
                let Some(job) = registry.jobs.get_mut(&job_id) else {
                    continue;
                };
                if job.start().is_err() {
                    eprintln!("Worker {}: failed to start job {}", self.id, job_id);
                    continue;
                }
                job.profile.clone()
            };

            // FFmpeg runs without any lock held. Input path must be the
            // canonical file (data/jobs/{id}/input/input.bin), not the
            // directory.
            let input_path: PathBuf = self
                .storage
                .job_input_path(&job_id)
                .join("input.bin");
            let output_path: PathBuf = self
                .storage
                .job_output_path(&job_id)
                .join("proxy.mp4");
            let result = self
                .ffmpeg_runner
                .run(&input_path, &output_path, &profile_name)
                .await;

            // Apply terminal status under a narrow registry guard.
            let mut registry = self.registry.lock().await;
            if let Some(job) = registry.jobs.get_mut(&job_id) {
                match result {
                    Ok(()) => {
                        if job.succeed(output_path).is_err() {
                            eprintln!(
                                "Worker {}: failed to mark job {} succeeded",
                                self.id, job_id
                            );
                        }
                    }
                    Err(e) => {
                        if job.fail(e.to_string()).is_err() {
                            eprintln!(
                                "Worker {}: failed to mark job {} failed",
                                self.id, job_id
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct WorkerPool {
    workers: Vec<Worker>,
}

impl WorkerPool {
    pub fn new(
        size: usize,
        registry: RegistryArc,
        queue_rx: JobReceiver,
        ffmpeg_runner: FfmpegRunner,
        storage: Arc<Storage>,
    ) -> Self {
        let workers = (0..size)
            .map(|id| {
                Worker::new(
                    id,
                    registry.clone(),
                    queue_rx.clone(),
                    ffmpeg_runner.clone(),
                    storage.clone(),
                )
            })
            .collect();

        WorkerPool { workers }
    }

    pub async fn start(&self) {
        let workers: Vec<_> = self.workers.to_vec();
        for worker in workers {
            tokio::spawn(async move {
                if let Err(e) = worker.run().await {
                    eprintln!("Worker {} error: {}", worker.id, e);
                }
            });
        }
    }
}
