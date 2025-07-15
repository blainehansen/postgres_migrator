FROM rust:alpine AS builder
WORKDIR /usr/src/

RUN apk add --no-cache musl-dev openssl-dev pkgconfig openssl-libs-static

RUN USER=root cargo new postgres_migrator
WORKDIR /usr/src/postgres_migrator
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

COPY src ./src
RUN cargo install --path .


FROM python:3.13-alpine
RUN pip install migra~=3.0.0 psycopg2-binary~=2.9.3 setuptools==79.0.1

COPY --from=builder /usr/local/cargo/bin/postgres_migrator /usr/bin/

WORKDIR /working

ENTRYPOINT ["/usr/bin/postgres_migrator"]
