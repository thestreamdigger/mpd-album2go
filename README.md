# Album2Go

[![MPD](https://img.shields.io/badge/MPD-compatible-brightgreen)](https://www.musicpd.org/)
[![Python](https://img.shields.io/badge/Python-3.7%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-GPL%20v3-blue)](https://www.gnu.org/licenses/gpl-3.0)

A simple tool to automatically copy the currently playing album from MPD to a connected USB device. Designed to run on Raspberry Pi.

## Description

**Album2Go** is a compact and self-contained script that automatically detects the album currently being played by MPD and copies it to a connected USB drive. All functionality is contained in a single script for easy installation and use.

## Requirements

- Python 3.7 or higher
- MPD server installed and configured on Raspberry Pi
- USB drive (minimum 4GB recommended)
- Libraries: `python-mpd2` and `psutil`

## Installation

1. Install the necessary dependencies:

```bash
pip install python-mpd2 psutil
```

Or using the requirements file:

```bash
pip install -r requirements.txt
```

2. Download the `a2g.py` file and place it in a directory of your choice on your Raspberry Pi.

## Usage

```bash
# Give execution permission
chmod +x a2g.py

# Run 
./a2g.py
```

## Built-in Settings

The script uses built-in settings that can be modified directly in the script file if needed.

Default configuration:

```json
{
  "mpd": {
    "host": "localhost",
    "port": 6600,
    "timeout": 10
  },
  "copy": {
    "min_usb_size_gb": 4,
    "safety_margin_mb": 200,
    "path_structure": {
      "music_root": "/var/lib/mpd/music"
    },
    "metadata_dirs": ["Artwork", "Covers", "Scans", ".thumbnails", "Booklets"]
  },
  "performance": {
    "buffer_size_mb": 16,
    "flush_interval_mb": 128,
    "min_flush_interval_sec": 0.5,
    "large_file_threshold_mb": 10,
    "progress_update_interval_sec": 0.5
  },
  "logging": {
    "level": "INFO",
    "format": "[{level}] {message}"
  }
}
```

## Features

- **Simple usage**: A single command copies the current album
- **Zero interaction**: No questions or choices, the script runs automatically
- **USB detection**: Automatically finds the eligible USB device
- **Progress bar**: Shows copy progress in real-time
- **Organized structure**: Maintains the Artist/Album structure on the device
- **High performance**: Optimized for fast and efficient copying
- **Self-contained**: Single file with no external configuration needed

## Based on

This script is a simplified version of the [MPD-Takeaway](https://github.com/thestreamdigger/mpd-takeaway) project, keeping only the current album copy functionality in a single file. 