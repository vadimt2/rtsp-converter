FROM python:3.11-slim

# Install FFmpeg and yt-dlp dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install yt-dlp
RUN pip install --no-cache-dir yt-dlp

WORKDIR /app

COPY stream.py .
COPY config.env .

CMD ["python", "-u", "stream.py"]
