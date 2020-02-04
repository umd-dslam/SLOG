#!/bin/bash

# stop on error
set -e

# Install toolings to compile dependencies
sudo apt install autoconf automake libtool curl make g++ unzip libreadline-dev pkg-config || true

INSTALL_PREFIX=$PWD/.deps
DOWNLOAD_DIR=$INSTALL_PREFIX/download
mkdir -p $INSTALL_PREFIX
cd $INSTALL_PREFIX

if [ -n "$(find $INSTALL_PREFIX -name 'libzmq.a')" ]; then
  echo "Found zmq. Skipping installation."
else
  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR

  echo "Downloading zmq"
  wget -nc https://github.com/zeromq/libzmq/releases/download/v4.3.2/zeromq-4.3.2.tar.gz 
  tar -xzf zeromq-4.3.2.tar.gz
  rm -f zeromq-4.3.2.tar.gz

  echo "Installing zmq"
  cd zeromq-4.3.2
  mkdir -p build
  cd build
  cmake .. -DWITH_PERF_TOOL=OFF -DZMQ_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX
  make -j$(nproc) install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if [ -n "$(find $INSTALL_PREFIX -name 'cppzmq*')" ]; then
  echo "Found cppzmq. Skipping installation."
else
  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading cppzmq"
  wget -nc https://github.com/zeromq/cppzmq/archive/v4.5.0.tar.gz
  tar -xzf v4.5.0.tar.gz
  rm -rf v4.5.0.tar.gz

  echo "Installing cppzmq"
  cd cppzmq-4.5.0
  mkdir -p build
  cd build
  cmake .. -DCMAKE_INSTALL_PREFIX:PATH=$INSTALL_PREFIX
  make -j$(nproc) install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if [ -n "$(find $INSTALL_PREFIX -name 'libprotobuf*')" ]; then
  echo "Found protobuf. Skipping installation."
else
  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading protobuf"
  wget -nc https://github.com/protocolbuffers/protobuf/releases/download/v3.11.1/protobuf-cpp-3.11.1.tar.gz
  tar -xzf protobuf-cpp-3.11.1.tar.gz
  rm -f protobuf-cpp-3.11.1.tar.gz

  echo "Installing protobuf"
  cd protobuf-3.11.1
  ./autogen.sh
  ./configure --prefix=$INSTALL_PREFIX
  make -j$(nproc) install
  cd ..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if [ -n "$(find $INSTALL_PREFIX -name 'libgflags*')" ]; then
  echo "Found gflags. Skipping installation."
else
  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading gflags"
  wget -nc https://github.com/gflags/gflags/archive/v2.2.2.tar.gz
  tar -xzf v2.2.2.tar.gz
  rm -r v2.2.2.tar.gz

  echo "Installing gflags"
  cd gflags-2.2.2
  mkdir -p build-tmp # folder has file named BUILD, macOS is not case-sensitive
  cd build-tmp
  cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX ..
  make -j$(nproc) install
  cd ..
  cd ..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if [ -n "$(find $INSTALL_PREFIX -name 'libglog*')" ]; then
  echo "Found glog. Skipping installation."
else
  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading glog"
  wget -nc https://github.com/google/glog/archive/v0.3.4.tar.gz
  tar -xzf v0.3.4.tar.gz
  rm -r v0.3.4.tar.gz

  echo "Installing glog"
  cd glog-0.3.4
  ./configure --prefix=$INSTALL_PREFIX
  make -j$(nproc) install
  cd ..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi

if [ -n "$(find $INSTALL_PREFIX -name 'libgtest*')" ]; then
  echo "Found gtest. Skipping installation."
else
  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR
  echo "Downloading gtest"
  wget -nc https://github.com/google/googletest/archive/release-1.10.0.tar.gz
  tar -xzf release-1.10.0.tar.gz
  rm -r release-1.10.0.tar.gz

  echo "Installing gtest"
  cd googletest-release-1.10.0
  mkdir build && cd build
  cmake .. -DBUILD_SHARED_LIBS=ON -DINSTALL_GTEST=ON -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX
  make -j$(nproc) install
  cd ..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi