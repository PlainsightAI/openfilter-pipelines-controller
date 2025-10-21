#!/bin/bash
set -e

VIDEO_URL="${VIDEO_URL:-https://videos.pexels.com/video-files/855564/855564-hd_1280_720_24fps.mp4}"
VIDEO_FILE="/videos/video.mp4"
STREAM_NAME="${STREAM_NAME:-stream}"

echo "Starting RTSP streaming service..."
echo "Video URL: $VIDEO_URL"
echo "Stream name: $STREAM_NAME"

# Download video if not already present
if [ ! -f "$VIDEO_FILE" ]; then
    echo "Downloading video from $VIDEO_URL..."
    curl -L -o "$VIDEO_FILE" "$VIDEO_URL"
    echo "Download complete!"
else
    echo "Video already downloaded at $VIDEO_FILE"
fi

# Start MediaMTX in the background
echo "Starting MediaMTX server..."
/usr/local/bin/mediamtx /mediamtx.yml &
MEDIAMTX_PID=$!

# Wait for MediaMTX to be ready
echo "Waiting for MediaMTX to start..."
sleep 3

# Start FFmpeg to stream the video in a loop
echo "Starting FFmpeg stream loop..."
ffmpeg -re -stream_loop -1 -i "$VIDEO_FILE" \
    -c:v copy \
    -c:a copy \
    -f rtsp \
    -rtsp_transport tcp \
    "rtsp://localhost:8554/$STREAM_NAME" &
FFMPEG_PID=$!

echo "RTSP stream is now available at rtsp://<pod-ip>:8554/$STREAM_NAME"

# Function to handle shutdown
cleanup() {
    echo "Shutting down..."
    kill $FFMPEG_PID 2>/dev/null || true
    kill $MEDIAMTX_PID 2>/dev/null || true
    exit 0
}

trap cleanup SIGTERM SIGINT

# Wait for either process to exit
wait -n $MEDIAMTX_PID $FFMPEG_PID
EXIT_CODE=$?

# If one exits, clean up the other
cleanup
