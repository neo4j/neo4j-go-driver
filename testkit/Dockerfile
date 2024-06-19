FROM ubuntu:20.04

ARG go_version=1.19

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
    && wget --quiet https://golang.org/dl/go${go_version}.linux-amd64.tar.gz \
    && tar -xzf go${go_version}.linux-amd64.tar.gz \
    && mv go /usr/local

ENV GOROOT=/usr/local/go
ENV PATH=$GOROOT/bin:$PATH

# Install python stuff
ENV PYTHON=python3

# Install our own CAs on the image.
# Assumes Linux Debian based image.
COPY CAs/* /usr/local/share/ca-certificates/
RUN update-ca-certificates
