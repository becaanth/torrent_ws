FROM ubuntu:22.04

# 1. Install system dependencies
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libssl-dev \
    libboost-python-dev \
    libboost-system-dev

RUN apt update && apt install -q -y python3 python3-pip python3-distutils

RUN apt update && apt install -q -y vim
ARG GROUPID=0
ARG USERID=0
USER 0:0
ARG USERNAME=root
ARG HOMEDIR=/root
USER ${USERID}:${GROUPID}

ENV VTRROOT=${HOMEDIR}/ASRL/vtr3
ENV VTRSRC=${VTRROOT}/src \
  VTRDATA=${VTRROOT}/data \
  VTRTEMP=${VTRROOT}/temp \
  VTRMODELS=${VTRROOT}/models \
  GRIZZLY=${VTRROOT}/grizzly \
  WARTHOG=${VTRROOT}/warthog \
  VTRUI=${VTRSRC}/main/src/vtr_gui/vtr_gui/vtr-gui
  
# 2. Clone libtorrent RC_2_0
WORKDIR /root
RUN git clone --recursive https://github.com/arvidn/libtorrent.git -b RC_2_0

# 3. Build using CMake
WORKDIR /root/libtorrent/build
RUN cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DTORRENT_USE_ASSERTS=ON \
      -Dpython-bindings=ON \
      -Dencryption=ON \
      -Ddht=ON \
      -Dcrypto=openssl \
      -Dextensions=ON \
      -Dmutable-torrents=ON \
      -DPYTHON_EXECUTABLE=/usr/bin/python3.10 \
      -DCMAKE_CXX_FLAGS="-DTORRENT_USE_OPENSSL=1 -DTORRENT_USE_ED25519=1 -DTORRENT_HAS_DONNA_ED25519=1 -fvisibility=default" \
      .. && \
    make -j$(nproc)

# 4. Verification Step inside the container
RUN nm -D bindings/python/libtorrent*.so | grep -E "dht_put_item|ed25519"
RUN cp ~/libtorrent/build/bindings/python/libtorrent*.so /usr/local/lib/python3.10/dist-packages/libtorrent.so

RUN pip install pynacl
RUN apt update && apt install -q -y iputils-ping net-tools
CMD ["/bin/bash"]
