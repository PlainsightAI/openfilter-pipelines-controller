# RTSP Video Stream Deployment

This directory contains a complete Kubernetes deployment for streaming a video file via RTSP using MediaMTX.

## Overview

The deployment:
- Downloads a video from Pexels (https://videos.pexels.com/video-files/855564/855564-hd_1280_720_24fps.mp4)
- Streams it in a continuous loop via RTSP using MediaMTX
- Exposes RTSP, API, and metrics endpoints

## Files

- `Dockerfile` - Multi-stage Docker image with MediaMTX and FFmpeg
- `mediamtx.yml` - MediaMTX server configuration
- `entrypoint.sh` - Startup script that downloads video and starts streaming
- `deployment.yaml` - Kubernetes Deployment manifest
- `service.yaml` - Kubernetes Service manifests (ClusterIP and optional NodePort)

## Quick Start

### 1. Build the Docker Image

```bash
cd deploy/rtsp-stream
docker build -t rtsp-video-stream:latest .
```

If using a local Kubernetes cluster (like Kind or Minikube), load the image:

```bash
# For Kind
kind load docker-image rtsp-video-stream:latest

# For Minikube
minikube image load rtsp-video-stream:latest
```

### 2. Deploy to Kubernetes

```bash
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
```

### 3. Access the Stream

#### From within the cluster:

```bash
rtsp://rtsp-video-stream.default.svc.cluster.local:8554/stream
```

#### From outside the cluster (NodePort):

Uncomment the NodePort service in `service.yaml` and apply it:

```bash
kubectl apply -f service.yaml
```

Then access via:

```bash
rtsp://<node-ip>:30554/stream
```

## Testing the Stream

### Using FFmpeg

```bash
ffmpeg -i rtsp://localhost:30554/stream -c copy output.mp4
```

### Using VLC

```bash
vlc rtsp://localhost:30554/stream
```

Or open VLC and go to Media > Open Network Stream and enter the RTSP URL.

### Using GStreamer

```bash
gst-launch-1.0 rtspsrc location=rtsp://localhost:30554/stream latency=0 ! decodebin ! autovideosink
```

## Configuration

### Environment Variables

You can customize the deployment by modifying environment variables in `deployment.yaml`:

- `VIDEO_URL` - URL of the video to download and stream (default: Pexels video)
- `STREAM_NAME` - Name of the RTSP stream path (default: "stream")

Example:

```yaml
env:
- name: VIDEO_URL
  value: "https://example.com/my-video.mp4"
- name: STREAM_NAME
  value: "mystream"
```

### MediaMTX Configuration

Edit `mediamtx.yml` to customize MediaMTX settings such as:
- RTSP transport protocols (TCP/UDP)
- Authentication
- Encryption
- API settings
- Metrics

## Endpoints

The deployment exposes three ports:

- **8554** - RTSP stream endpoint
- **9997** - MediaMTX API endpoint
- **9998** - Prometheus metrics endpoint

### API Examples

Get configuration:
```bash
curl http://rtsp-video-stream:9997/v3/config/global/get
```

Get path list:
```bash
curl http://rtsp-video-stream:9997/v3/paths/list
```

### Metrics

Access Prometheus metrics:
```bash
curl http://rtsp-video-stream:9998/metrics
```

## Resource Requirements

The deployment is configured with:

**Requests:**
- Memory: 256Mi
- CPU: 250m

**Limits:**
- Memory: 512Mi
- CPU: 500m

Adjust these values in `deployment.yaml` based on your video quality and expected load.

## Troubleshooting

### Check pod status

```bash
kubectl get pods -l app=rtsp-video-stream
```

### View logs

```bash
kubectl logs -l app=rtsp-video-stream -f
```

### Check if MediaMTX is running

```bash
kubectl exec -it <pod-name> -- wget -O- http://localhost:9997/v3/config/global/get
```

### Test RTSP connectivity from within pod

```bash
kubectl exec -it <pod-name> -- ffmpeg -i rtsp://localhost:8554/stream -t 5 -f null -
```

## Cleanup

```bash
kubectl delete -f deployment.yaml
kubectl delete -f service.yaml
```

## Notes

- The video is downloaded once on container startup and cached in an emptyDir volume
- If the container restarts, the video will be downloaded again
- For production use, consider using a PersistentVolume to cache the video
- The stream loops indefinitely using FFmpeg's `-stream_loop -1` option
- Both video and audio codecs are copied without re-encoding for efficiency
