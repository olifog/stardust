FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Base tools and build essentials
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    gnupg \
    software-properties-common \
    wget \
    build-essential \
    pkg-config \
    git \
    ninja-build \
    gdb \
    strace \
    lsb-release \
  && rm -rf /var/lib/apt/lists/*

# Install CMake 3.30 from Kitware APT repo (matches CI)
RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor -o /usr/share/keyrings/kitware-archive-keyring.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/kitware.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends cmake \
  && rm -rf /var/lib/apt/lists/*

# Runtime and dev libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    capnproto \
    libcapnp-dev \
    liblmdb-dev \
    libatomic1 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Default to an interactive shell; mount your source into /workspace
CMD ["bash"]


