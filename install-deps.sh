#!/bin/bash

SUDO=sudo
FORCE_INSTALL=0

while getopts ":df" option; do
   case $option in
      d) unset SUDO
      ;;
      f) FORCE_INSTALL=1
      ;;
   esac
done

# stop on error
set -e

# Install toolings to compile dependencies
$SUDO apt-get update
$SUDO apt-get -y install autoconf automake libtool curl unzip libreadline-dev pkg-config wget || true

INSTALL_PREFIX=$PWD/.deps
DOWNLOAD_DIR=$INSTALL_PREFIX/download
mkdir -p $INSTALL_PREFIX
cd $INSTALL_PREFIX

function need_install {
  rm -rf $DOWNLOAD_DIR
  if [ $FORCE_INSTALL -eq 1 ]; then
    return 0
  fi
  if [ -n "$(find $INSTALL_PREFIX -name $2)" ]; then
    echo "Found $1. Skipping installation."
    return -1
  fi
  return 0
}

CMAKE="cmake -DCMAKE_BUILD_TYPE=release -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX"

if need_install 'zmq' 'libzmq.a'; then
  libzmq_ver=4.3.3

  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR

  echo "Downloading zmq"
  wget -nc https://github.com/zeromq/libzmq/releases/download/v${libzmq_ver}/zeromq-${libzmq_ver}.tar.gz 
  tar -xzf zeromq-${libzmq_ver}.tar.gz
  rm -f zeromq-${libzmq_ver}.tar.gz

  echo "Installing zmq"
  cd zeromq-${libzmq_ver}
  mkdir -p build
  cd build
  $CMAKE -DWITH_PERF_TOOL=OFF -DZMQ_BUILD_TESTS=OFF ..
  make -j$(nproc) install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if need_install 'cppzmq' 'cppzmq*'; then
  cppzmq_ver=4.7.1

  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading cppzmq"
  wget -nc https://github.com/zeromq/cppzmq/archive/v${cppzmq_ver}.tar.gz
  tar -xzf v${cppzmq_ver}.tar.gz
  rm -rf v${cppzmq_ver}.tar.gz

  echo "Installing cppzmq"
  cd cppzmq-${cppzmq_ver}
  mkdir -p build
  cd build
  $CMAKE -DCPPZMQ_BUILD_TESTS=OFF ..
  make -j$(nproc) install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if need_install 'protobuf' 'libprotobuf*'; then 
  protobuf_ver=3.14.0

  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading protobuf"
  wget -nc https://github.com/protocolbuffers/protobuf/releases/download/v${protobuf_ver}/protobuf-cpp-${protobuf_ver}.tar.gz
  tar -xzf protobuf-cpp-${protobuf_ver}.tar.gz
  rm -f protobuf-cpp-${protobuf_ver}.tar.gz

  echo "Installing protobuf"
  cd protobuf-${protobuf_ver}
  mkdir -p build
  cd build
  $CMAKE -Dprotobuf_BUILD_TESTS=OFF ../cmake
  make -j$(nproc) install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if need_install 'gflags' 'libgflags*'; then
  gflags_ver=2.2.2

  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading gflags"
  wget -nc https://github.com/gflags/gflags/archive/v${gflags_ver}.tar.gz
  tar -xzf v${gflags_ver}.tar.gz
  rm -r v${gflags_ver}.tar.gz

  echo "Installing gflags"
  cd gflags-${gflags_ver}
  mkdir -p build-tmp # folder has file named BUILD, macOS is not case-sensitive
  cd build-tmp
  $CMAKE -DGFLAGS_BUILD_TESTING=OFF -DGFLAGS_NAMESPACE="google" ..
  make -j$(nproc) install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if need_install 'glog' 'libglog*'; then
  glog_ver=0.4.0

  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading glog"
  wget -nc https://github.com/google/glog/archive/v${glog_ver}.tar.gz
  tar -xzf v${glog_ver}.tar.gz
  rm -r v${glog_ver}.tar.gz

  echo "Installing glog"
  cd glog-${glog_ver}
  mkdir -p build
  cd build
  $CMAKE -DBUILD_TESTING=OFF ..
  make -j$(nproc) install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if need_install 'rapidjson' 'rapidjson*'; then
  rapidjson_ver=1.1.0

  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading rapidjson"
  wget -nc https://github.com/Tencent/rapidjson/archive/refs/tags/v${rapidjson_ver}.tar.gz
  tar -xzf v${rapidjson_ver}.tar.gz
  rm -r v${rapidjson_ver}.tar.gz

  echo "Installing rapidjson"
  cd rapidjson-${rapidjson_ver}
  mkdir -p build
  cd build
  $CMAKE ..\
    -DRAPIDJSON_BUILD_EXAMPLES=OFF\
    -DRAPIDJSON_BUILD_TESTS=OFF\
    -DRAPIDJSON_BUILD_DOC=OFF\
    -DRAPIDJSON_BUILD_CXX17=ON
  make -j$(nproc) install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi


