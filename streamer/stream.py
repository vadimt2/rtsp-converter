#!/usr/bin/env python3
"""
Multi-stream extractor and RTSP publisher for road-ai.
Extracts HLS streams from YouTube/EarthCam and pushes to MediaMTX RTSP server.
Supports multiple concurrent streams from streams.json configuration.
"""

import subprocess
import os
import sys
import time
import signal
import threading
from typing import Optional, Dict, List
from dataclasses import dataclass

# Configuration
RTSP_SERVER = os.getenv("RTSP_SERVER", "localhost")
RTSP_PORT = os.getenv("RTSP_PORT", "8554")

# Retry settings
MAX_RETRIES = -1  # -1 = infinite
RETRY_DELAY = 10  # seconds
STREAM_URL_REFRESH = 3600  # Refresh stream URL every hour (YouTube URLs expire)


@dataclass
class StreamConfig:
    name: str
    source: str
    enabled: bool
    description: str = ""


class StreamWorker(threading.Thread):
    """Worker thread that handles a single stream."""

    def __init__(self, config: StreamConfig, rtsp_server: str, rtsp_port: str):
        super().__init__(daemon=True)
        self.config = config
        self.rtsp_url = f"rtsp://{rtsp_server}:{rtsp_port}/{config.name}"
        self.ffmpeg_process: Optional[subprocess.Popen] = None
        self.running = True
        self.last_url_fetch = 0
        self.cached_stream_url: Optional[str] = None

    def stop(self):
        """Signal the worker to stop."""
        self.running = False
        if self.ffmpeg_process:
            self.ffmpeg_process.terminate()
            try:
                self.ffmpeg_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.ffmpeg_process.kill()

    def get_stream_url(self) -> Optional[str]:
        """Extract the actual stream URL using yt-dlp with caching."""
        current_time = time.time()

        # Return cached URL if still valid
        if (self.cached_stream_url and
                current_time - self.last_url_fetch < STREAM_URL_REFRESH):
            return self.cached_stream_url

        print(f"[{self.config.name}] Extracting stream URL from: {self.config.source}")

        try:
            result = subprocess.run(
                [
                    "yt-dlp",
                    "-f", "best[ext=mp4]/best",
                    "-g",
                    "--no-warnings",
                    self.config.source
                ],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                stream_url = result.stdout.strip()
                if stream_url:
                    print(f"[{self.config.name}] Got stream URL: {stream_url[:60]}...")
                    self.cached_stream_url = stream_url
                    self.last_url_fetch = current_time
                    return stream_url

            print(f"[{self.config.name}] yt-dlp failed: {result.stderr}")
            return None

        except subprocess.TimeoutExpired:
            print(f"[{self.config.name}] yt-dlp timed out")
            return None
        except Exception as e:
            print(f"[{self.config.name}] Failed to extract stream: {e}")
            return None

    def start_ffmpeg(self, input_url: str) -> bool:
        """Start FFmpeg to restream to RTSP."""
        print(f"[{self.config.name}] Starting FFmpeg restream to: {self.rtsp_url}")

        ffmpeg_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            # Input settings
            "-re",  # Read input at native frame rate
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
            "-i", input_url,
            # Video: copy (no re-encode)
            "-c:v", "copy",
            # Audio: re-encode to AAC with global header for RTSP compatibility
            "-c:a", "aac",
            "-b:a", "128k",
            # RTSP output
            "-f", "rtsp",
            "-rtsp_transport", "tcp",
            self.rtsp_url
        ]

        try:
            self.ffmpeg_process = subprocess.Popen(
                ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            print(f"[{self.config.name}] FFmpeg started with PID: {self.ffmpeg_process.pid}")
            return True
        except Exception as e:
            print(f"[{self.config.name}] Failed to start FFmpeg: {e}")
            return False

    def run(self):
        """Main loop for this stream worker."""
        print(f"[{self.config.name}] Worker started for: {self.config.description}")
        retry_count = 0

        while self.running:
            # Check retry limit
            if MAX_RETRIES >= 0 and retry_count >= MAX_RETRIES:
                print(f"[{self.config.name}] Max retries reached. Worker stopping.")
                break

            # Extract stream URL
            stream_url = self.get_stream_url()
            if not stream_url:
                retry_count += 1
                print(f"[{self.config.name}] Retry {retry_count} in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
                continue

            # Start FFmpeg
            if not self.start_ffmpeg(stream_url):
                retry_count += 1
                print(f"[{self.config.name}] Retry {retry_count} in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
                continue

            # Reset retry count on successful start
            retry_count = 0
            stream_start_time = time.time()

            # Monitor FFmpeg process
            while self.running and self.ffmpeg_process:
                retcode = self.ffmpeg_process.poll()
                if retcode is not None:
                    # Process ended
                    stderr = self.ffmpeg_process.stderr.read().decode() if self.ffmpeg_process.stderr else ""
                    print(f"[{self.config.name}] FFmpeg exited with code {retcode}")
                    if stderr:
                        print(f"[{self.config.name}] FFmpeg stderr: {stderr[-300:]}")
                    # Invalidate cached URL on error
                    self.cached_stream_url = None
                    break

                # Check if we need to refresh the stream URL (YouTube URLs expire)
                if time.time() - stream_start_time > STREAM_URL_REFRESH:
                    print(f"[{self.config.name}] Refreshing stream URL...")
                    self.ffmpeg_process.terminate()
                    self.ffmpeg_process.wait(timeout=5)
                    self.cached_stream_url = None
                    break

                time.sleep(1)

            if self.running:
                print(f"[{self.config.name}] Restarting in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)

        print(f"[{self.config.name}] Worker stopped.")


class MultiStreamManager:
    """Manages multiple stream workers."""

    def __init__(self):
        self.workers: Dict[str, StreamWorker] = {}
        self.running = True

        # Handle graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        print(f"\n[MANAGER] Received signal {signum}, shutting down all streams...")
        self.running = False
        for name, worker in self.workers.items():
            print(f"[MANAGER] Stopping {name}...")
            worker.stop()

    def load_streams(self) -> List[StreamConfig]:
        """Load stream configurations from environment variables.

        Reads indexed env vars: STREAM_0_NAME, STREAM_0_SOURCE, etc.
        """
        streams = []
        index = 0

        while True:
            name = os.getenv(f"STREAM_{index}_NAME")
            source = os.getenv(f"STREAM_{index}_SOURCE")

            # Stop when we hit a gap in the sequence
            if not name or not source:
                break

            enabled_str = os.getenv(f"STREAM_{index}_ENABLED", "true").lower()
            enabled = enabled_str in ("true", "1", "yes", "on")
            description = os.getenv(f"STREAM_{index}_DESC", "")

            streams.append(StreamConfig(
                name=name,
                source=source,
                enabled=enabled,
                description=description
            ))

            index += 1

        if not streams:
            print("[MANAGER] No streams configured in environment")
            print("[MANAGER] Using default Abbey Road stream")
            return [StreamConfig(
                name="abbeyroad",
                source="https://www.youtube.com/watch?v=57w2gYXjRic",
                enabled=True,
                description="Abbey Road Crossing, London"
            )]

        return streams

    def wait_for_rtsp_server(self, timeout: int = 30) -> bool:
        """Wait for MediaMTX RTSP server to be ready."""
        import socket

        print(f"[MANAGER] Waiting for RTSP server at {RTSP_SERVER}:{RTSP_PORT}...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex((RTSP_SERVER, int(RTSP_PORT)))
                sock.close()

                if result == 0:
                    print("[MANAGER] RTSP server is ready!")
                    return True
            except Exception:
                pass

            time.sleep(1)

        print("[MANAGER] RTSP server not available")
        return False

    def run(self):
        """Start all enabled streams."""
        print("=" * 60)
        print("Road-AI Multi-Stream RTSP Server")
        print("=" * 60)

        # Wait for RTSP server
        if not self.wait_for_rtsp_server():
            sys.exit(1)

        # Load stream configs
        streams = self.load_streams()
        enabled_streams = [s for s in streams if s.enabled]

        if not enabled_streams:
            print("[MANAGER] No enabled streams found!")
            sys.exit(1)

        print(f"[MANAGER] Starting {len(enabled_streams)} stream(s):")
        for s in enabled_streams:
            print(f"  - {s.name}: {s.description}")
            rtsp_url = f"rtsp://{RTSP_SERVER}:{RTSP_PORT}/{s.name}"
            print(f"    RTSP: {rtsp_url}")

        print("=" * 60)

        # Start workers for each enabled stream
        for config in enabled_streams:
            worker = StreamWorker(config, RTSP_SERVER, RTSP_PORT)
            self.workers[config.name] = worker
            worker.start()
            # Stagger starts to avoid overwhelming yt-dlp
            time.sleep(2)

        # Keep main thread alive and monitor workers
        while self.running:
            time.sleep(5)

            # Check if all workers are still alive
            alive_count = sum(1 for w in self.workers.values() if w.is_alive())
            if alive_count == 0:
                print("[MANAGER] All workers stopped. Exiting.")
                break

        # Wait for workers to finish
        for worker in self.workers.values():
            worker.join(timeout=10)

        print("[MANAGER] Shutdown complete.")


if __name__ == "__main__":
    manager = MultiStreamManager()
    manager.run()
