# syntax=docker/dockerfile:1

FROM rust:1-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin tangleveil

WORKDIR /app
COPY --from=builder /app/target/release/tangleveil /usr/local/bin/tangleveil
COPY static ./static
COPY config.example.toml sources.example.toml ./

USER tangleveil
ENV RUST_LOG=tangleveil=info
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health || exit 1

ENTRYPOINT ["tangleveil"]
CMD ["--config", "config.toml"]
