**!!! This repo is a Work In Progress !!!**

# SLOG

This is an experimental implementation of the paper [SLOG: serializable, low-latency, geo-replicated transactions](http://www.vldb.org/pvldb/vol12/p1747-ren.pdf).

The following instructions work best on Ubuntu with CMake 3.13.4.

## How to Build

You need to install the dependencies first by running `install_deps.sh`. This script installs all dependencies in the 
`.dep` directory at the root of the project directory. After that, run the following commands from the root of the 
project

```
mkdir build
cd build
cmake ..
make -j
```

## How to Run Tests 
To run tests, from the 'build' directory, run
```
ctest
```

## How to Start a Database

### As multiple processes on a single machine

This set up is only for testing on the local machine. In the following example config file (slog.conf), we use 2 
replicas, each of which has two partitions.

```
protocol: "icp"
addresses: "/tmp/test_0"
addresses: "/tmp/test_1"
addresses: "/tmp/test_2"
addresses: "/tmp/test_3"
broker_port: 0
num_replicas: 2
num_partitions: 2
```

Open 4 terminal windows and run each of the following commands in each of the terminal. The current directory should be 
the root of the project:
```
./build/slog -address /tmp/test_0 -replica 0 -partition 0
```
```
./build/slog -address /tmp/test_1 -replica 0 -partition 1
```
```
./build/slog -address /tmp/test_2 -replica 1 -partition 0
```
```
./build/slog -address /tmp/test_3 -replica 1 -partition 1
```

Note that there is no restriction in which address should run what replica or partition. As long as all partitions of
all replicas are up then the whole system will work.
