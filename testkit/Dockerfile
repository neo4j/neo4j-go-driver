ARG TIME_WARP=""
FROM ubuntu:24.04 AS base

ARG go_version_min=1.24.6
ARG go_version_default=1.24.6
# From https://go.dev/dl, change together with go_version_default
ARG go_sha256_default=bbca37cc395c974ffa4893ee35819ad23ebb27426df87af92e93a9ec66ef8712

# Install all needed to build and run tests
# Install tzdata to make sure Go timezone info works correctly (need noninteractive to avoid
# timezone prompt when installing)
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y \
        git \
        tzdata \
        python3 \
        wget \
        gcc \
    && rm -rf /var/lib/apt/lists/* \
    # the packaged Golang version of Ubuntu is too old (requires manual install of gcc first)
    && wget --quiet https://golang.org/dl/go${go_version_default}.linux-amd64.tar.gz \
    && echo "${go_sha256_default}  go${go_version_default}.linux-amd64.tar.gz" | sha256sum -c - \
    && tar -xzf go${go_version_default}.linux-amd64.tar.gz \
    && mv go /usr/local

ENV GOROOT=/usr/local/go
ENV PATH=$GOROOT/bin:/root/go/bin:$PATH
ENV GOMINBIN=go${go_version_min}

# Install minimum version of Go alongside latest, verified by the Go checksum database
RUN root="$(GOTOOLCHAIN=go${go_version_min} go env GOROOT)" \
    && mkdir -p /root/go/bin \
    && printf '#!/bin/sh\nGOROOT=%s exec %s/bin/go "$@"\n' "$root" "$root" > /root/go/bin/${GOMINBIN} \
    && chmod +x /root/go/bin/${GOMINBIN}


FROM base AS backend-timewarp
ARG TIME_WARP
WORKDIR /home/root/testkit
COPY testkit-backend .
RUN go mod init github.com/neo4j/neo4j-go-driver/testkit-backend/v2 \
    && go get github.com/neo4j/neo4j-go-driver/v6@v${TIME_WARP} \
    && echo tags: $(./build_tags.sh ${TIME_WARP}) \
    && go build -tags "$(./build_tags.sh ${TIME_WARP})" -o backend
ENTRYPOINT ["./backend"]

FROM base AS backend
# Install python stuff
ENV PYTHON=python3

# Install our own CAs on the image.
# Assumes Linux Debian based image.
COPY CAs/* /usr/local/share/ca-certificates/
RUN update-ca-certificates

FROM backend${TIME_WARP:+"-timewarp"} AS final
ARG TIME_WARP
ENV DRIVER_TIME_WARP=$TIME_WARP
WORKDIR /home/root/testkit
EXPOSE 9876/tcp
