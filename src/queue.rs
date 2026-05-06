use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

pub type JobSender = mpsc::UnboundedSender<Uuid>;
pub type JobReceiver = Arc<Mutex<mpsc::UnboundedReceiver<Uuid>>>;

pub fn create_queue() -> (JobSender, JobReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (tx, Arc::new(Mutex::new(rx)))
}
