# Video Digest Server 🎥

[![Rust](https://img.shields.io/badge/language-Rust-orange.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance video ingestion and transcoding server designed to act as a bridge between raw camera footage and the editing suite. This server automatically processes uploaded high-bitrate files into lightweight proxies or professional mezzanine formats, ensuring a smooth non-linear editing (NLE) experience.

## 📖 Overview

The **Video Digest Server** is built in Rust to leverage memory safety and fearless concurrency. Its primary role is to handle large video uploads from various camera sources, queue them for processing, and encode them based on predefined profiles. 

By converting heavy raw files into "proxies," editors can work with low-resolution versions of their footage without straining system resources, later relinking to the original high-resolution files for final render.

## ✨ Features

- **Async Processing Pipeline**: Built with `Tokio` to handle multiple simultaneous uploads and encoding jobs.
- **Dynamic Profile Management**: Switch between "Quick Proxy" and "Professional Mezzanine" formats via configuration.
- **FFmpeg Integration**: Leverages the power of FFmpeg for industry-standard codec support.
- **Queue System**: Prevents server saturation by managing transcoding tasks in a priority queue.
- **Hardware Acceleration**: Supports NVENC/QuickSync (via FFmpeg) for faster encoding.

## 🛠 Build Steps

### Prerequisites

- **Rust**: Install via [rustup](https://rustup.rs/) (`cargo`).
- **FFmpeg**: Must be installed on the system path with the required codecs enabled.
  - Linux: `sudo apt install ffmpeg`
  - macOS: `brew install ffmpeg`

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/youruser/video-digest-server.git
   cd video-digest-server
   ```

2. Configure environment variables:
   ```bash
   cp .env.example .env
   # Edit .env to set upload directories and port settings
   ```

3. Build the project:
   ```bash
   cargo build --release
   ```

4. Run the server:
   ```bash
   cargo run --release
   ```

## ⚙️ Encoding Profiles

The server allows you to define profiles in `config.toml`. Depending on the project requirements, you can choose between lightweight web-ready proxies or professional intermediate codecs.

### 1. General Proxy (Fast & Lightweight)
Designed for remote collaboration and low-spec laptops.

| Feature | Value |
| :--- | :--- |
| **Codec** | H.264 / AAC |
| **Container** | `.mp4` or `.mov` |
| **Resolution** | 1280x720 (720p) |
| **Bitrate** | 2-5 Mbps |

### 2. Professional Mezzanine (Edit Ready)
Designed for high-end post-production houses where visual fidelity and scrubbability are more important than file size.

| Format | Codec | Container | Use Case |
| :--- | :--- | :--- | :--- |
| **Apple ProRes** | `prores_ks` | `.mov` | Industry standard for macOS/Final Cut / Premiere |
| **Avid DNxHD** | `dnxhd` | `.mxf` | Optimized for Avid Media Composer |
| **CineForm** | `cineform` | `.mov` | High-quality intermediate with alpha support |
| **APV** | `apv` | `.mov` | Specialized high-efficiency professional format |

## 🚀 Examples

### API Upload Example
You can trigger a digest job by sending a POST request to the server:

```bash
curl -X POST http://localhost:8080/upload \
  -F "video=@/path/to/camera_raw_01.R3D" \
  -F "profile=prores_proxy"
```

### Configuration Example (`config.toml`)
```toml
[profiles.web_proxy]
codec = "libx264"
container = "mp4"
resolution = "1280x720"
crf = 23

[profiles.prores_proxy]
codec = "prores_ks"
profile = 1 # ProRes Proxy
container = "mov"
resolution = "1920x1080"

[profiles.avid_dnxhd]
codec = "dnxhd"
container = "mxf"
resolution = "1920x1080"
```

## 📜 License

Distributed under the MIT License. See `LICENSE` for more information.
