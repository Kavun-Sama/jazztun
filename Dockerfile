# syntax=docker/dockerfile:1.7

FROM golang:1.25-bookworm AS build

ARG VERSION=dev
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath \
      -ldflags="-s -w -X github.com/Kavun-Sama/jazztun/internal/buildinfo.Version=${VERSION}" \
      -o /out/server ./cmd/server

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/server /usr/local/bin/server

ENTRYPOINT ["/usr/local/bin/server"]
CMD ["-room", "new", "-peers", "4"]
