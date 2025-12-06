# syntax=docker/dockerfile:1.6

ARG SNAPCAST_VERSION=0.31.0

FROM debian:bookworm-slim AS build
ARG SNAPCAST_VERSION
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        libasound2-dev \
        libavahi-client-dev \
        libboost-filesystem-dev \
        libboost-system-dev \
        libexpat1-dev \
        libflac-dev \
        libmpg123-dev \
        libopus-dev \
        libpulse-dev \
        libssl-dev \
        libvorbis-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN curl -L -o snapcast.tar.gz https://github.com/badaix/snapcast/archive/refs/tags/v${SNAPCAST_VERSION}.tar.gz \
    && tar -xf snapcast.tar.gz --strip-components=1 \
    && cmake -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/opt/snapcast \
        -DBUILD_CLIENT=OFF \
        -DBUILD_SERVER=ON \
        -DBUILD_TESTS=OFF \
        -DBUILD_DOC=OFF \
    && cmake --build build --target snapserver --parallel \
    && cmake --install build

FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libasound2 \
        libavahi-client3 \
        libexpat1 \
        libflac12 \
        libmpg123-0 \
        libopus0 \
        libpulse0 \
        libssl3 \
        libvorbis0a \
        libvorbisenc2 \
        libvorbisfile3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/snapcast /opt/snapcast
ENV PATH="/opt/snapcast/bin:${PATH}"
EXPOSE 1704 1780 1705
ENTRYPOINT ["snapserver"]
