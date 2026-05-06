use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

pub const DEFAULT_MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(name = "syspref-ingest")]
#[command(about = "Video Digest Server - concurrent video upload and transcoding")]
pub struct Config {
    /// Path to FFmpeg profiles YAML file
    #[arg(short, long)]
    pub profiles_path: Option<PathBuf>,

    /// Base directory for storing files
    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// HTTP server address
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    pub bind: String,

    /// Per-request multipart body size limit in bytes. Applies to the entire
    /// encoded multipart body (boundary delimiters + part headers + file
    /// data). This is a per-request cap, not a concurrency limiter.
    #[arg(long, default_value_t = DEFAULT_MAX_UPLOAD_BYTES)]
    pub max_upload_bytes: usize,
}

impl Config {
    pub fn load() -> Result<Self> {
        Ok(Config::parse())
    }
}
