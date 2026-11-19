# Imagen del microservicio MicroserviceDB (HTTP/WebSocket + gRPC).
# Se construye desde scripts/install/docker-compose.yml (servicio "microservicedb").
FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml build.rs ./
COPY proto ./proto
COPY src ./src
COPY MicroserviceDB.html ./
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates p7zip-full \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/microservice-db /usr/local/bin/microservice-db
# 3329: HTTP + WebSocket (/ws) + UI   |   3330: gRPC
EXPOSE 3329 3330
ENTRYPOINT ["/usr/local/bin/microservice-db"]
