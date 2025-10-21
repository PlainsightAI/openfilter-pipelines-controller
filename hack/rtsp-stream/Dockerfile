FROM bluenviron/mediamtx:latest AS mediamtx

FROM alpine:3.19

# Install ffmpeg and curl
RUN apk add --no-cache ffmpeg curl bash

# Copy mediamtx binary from official image
COPY --from=mediamtx /mediamtx /usr/local/bin/mediamtx

# Create directory for video storage
RUN mkdir -p /videos

# Copy configuration and startup script
COPY mediamtx.yml /mediamtx.yml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose RTSP port
EXPOSE 8554

ENTRYPOINT ["/entrypoint.sh"]
