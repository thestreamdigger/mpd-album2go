# Changelog
All notable changes to the Album2Go project will be documented in this file.

## [0.1.0] - 2023-03-08

### Added
- Main functionality to copy the currently playing album from MPD to a USB device
- Automatic detection of USB devices with sufficient space
- Real-time progress bar with remaining time estimation
- Organized folder structure (Artist/Album) on the destination device
- Support for optimized buffer mechanisms for large files
- Configurable logging system
- Handling of metadata directories (covers, scans, etc.)
- Available space verification on the destination device
- Mechanism to detect already existing files to avoid duplicate copies

### Technical Requirements
- Python 3.7 or higher
- Dependencies: python-mpd2 (>=3.0.0), psutil (>=5.9.0)
- MPD server configured and running 