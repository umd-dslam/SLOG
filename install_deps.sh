#!/bin/bash

# stop on error
set -e

# sudo apt install libgtest-dev

INSTALL_PREFIX=$PWD/.dep
DOWNLOAD_DIR=$INSTALL_PREFIX/download
mkdir -p $INSTALL_PREFIX
cd $INSTALL_PREFIX

if [ -n "$(find $INSTALL_PREFIX -name 'libzmq.a')" ]; then
  echo "Found libzmq. Skipping installation."
else
  mkdir -p $DOWNLOAD_DIR
  cd $DOWNLOAD_DIR

  echo "Downloading libzmq"
  wget https://github.com/zeromq/libzmq/releases/download/v4.3.2/zeromq-4.3.2.tar.gz 
  tar -xzf zeromq-4.3.2.tar.gz
  rm -f zeromq-4.3.2.tar.gz

  echo "Installing libzmq"
  cd zeromq-4.3.2
  mkdir -p build
  cd build
  cmake .. -D WITH_PERF_TOOL=OFF -D ZMQ_BUILD_TESTS=OFF -D CMAKE_INSTALL_PREFIX:PATH=$INSTALL_PREFIX
  make -j install
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
  wget https://github.com/zeromq/cppzmq/archive/v4.5.0.tar.gz
  tar -xzf v4.5.0.tar.gz
  rm -rf v4.5.0.tar.gz

  echo "Installing cppzmq"
  cd cppzmq-4.5.0
  mkdir -p build
  cd build
  cmake .. -D CMAKE_INSTALL_PREFIX:PATH=$INSTALL_PREFIX
  make -j install
  cd ../..

  cd ..
  rm -rf $DOWNLOAD_DIR
fi