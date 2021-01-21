FROM ubuntu:20.04

# Install all needed to build and run tests
# This will install the default distribution Go version, for 18.04 this means 1.10
# Install tzdata to make sure Go timezone info works correctly (need noninteractive to avoid
# timezone prompt when installing)
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y \
        git \
        tzdata \
        python3 \
        golang \
    && rm -rf /var/lib/apt/lists/*

# Install python stuff
ENV PYTHON=python3

# Install our own CAs on the image.
# Assumes Linux Debian based image.
COPY CAs/* /usr/local/share/ca-certificates/
RUN update-ca-certificates
