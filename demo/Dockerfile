# ---------- build stage ----------
FROM rust:latest AS build
WORKDIR /app

# Cache dependencies first.
COPY Cargo.toml ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real binary.
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---------- runtime stage ----------
FROM debian:bookworm-slim

# Tools the demos show off. Everything here is what a learner would actually
# touch in a lesson; keep the image lean.
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
        htop \
        vim \
        less \
        procps \
        ncurses-base \
        ncurses-term \
        ca-certificates \
        iputils-ping \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash demo

COPY --from=build /app/target/release/xterm-server /usr/local/bin/xterm-server
COPY welcome.txt /etc/fenrir/welcome.txt

# Drop privileges. The shells handed out to the browser run as this user.
USER demo
WORKDIR /home/demo
ENV TERM=xterm-256color

EXPOSE 8080

# host/port are fixed; the per-terminal command comes from docker-compose.
ENTRYPOINT ["xterm-server", "--host", "0.0.0.0", "--port", "8080"]
CMD ["--cmd", "bash -i"]
