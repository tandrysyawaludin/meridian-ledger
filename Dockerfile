# syntax=docker/dockerfile:1

# Build stage: compile a static binary. CGO is off so the result runs on
# a distroless image with no libc, and the module cache is kept in its
# own layer so dependency downloads are not repeated on every source edit.
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /meridian ./cmd/meridian

# Runtime stage: a minimal, non-root image containing only the binary.
# There is no shell, so there is no HEALTHCHECK here — liveness and
# readiness are covered by the HTTP probes in deploy/k8s/deployment.yaml.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /meridian /meridian
EXPOSE 8080 9090
ENTRYPOINT ["/meridian"]
