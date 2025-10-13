# Build stage
FROM golang:1.25.1 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.mod
COPY go.sum go.sum

# Cache dependencies
RUN go mod download

# Copy source code
COPY cmd/claimer/ cmd/claimer/

# Build the binary
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o claimer cmd/claimer/main.go

# Runtime stage - use distroless for minimal attack surface
FROM gcr.io/distroless/static:nonroot

WORKDIR /

# Copy the binary from builder
COPY --from=builder /workspace/claimer /claimer

# Use nonroot user
USER 65532:65532

ENTRYPOINT ["/claimer"]
