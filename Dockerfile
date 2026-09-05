# Video pipeline API — multi-stage build.
#
#   Build:  pure-Go toolchain (modernc.org/sqlite needs no CGO, so
#           CGO_ENABLED=0 is safe and keeps the binary portable).
#   Run:    slim Debian + ffmpeg/ffprobe (transcoding) + CA certs (B2 TLS).
#           Render injects $PORT; the app reads it via config. SQLite lives
#           on the mounted disk at /data (see render.yaml, DB_PATH).

FROM golang:1.25-bookworm AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /server ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update \
	&& apt-get install -y --no-install-recommends ffmpeg ca-certificates \
	&& rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /server /app/server
EXPOSE 8080
CMD ["/app/server"]
