# SLOG

[![Build Status](https://github.com/ctring/SLOG/workflows/Build%20and%20Test/badge.svg)](https://github.com/ctring/SLOG/actions)

# What is SLOG?

SLOG is a geographically distributed data store that achieves high-throughput and low-latency transactions while guaranteeing strict serializability. 
For more details, see this [blog post](http://dbmsmusings.blogspot.com/2019/10/introducing-slog-cheating-low-latency.html) or the [SLOG paper](http://www.vldb.org/pvldb/vol12/p1747-ren.pdf).

This repository contains an experimental implementation of the system, which is not suitable for use in production.

# Getting Started 

The following guide has been tested on Ubuntu 20.04 with GCC 9.3.0 and CMake 3.16.3. Additional docs are in [the Wiki](https://github.com/ctring/SLOG/wiki).

## Build SLOG

First, install the build tools:
```
sudo apt install cmake build-essential pkg-config
```

Run the following commands to build the system. The dependencies will be downloaded and built automatically.

```
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_BUILD_TYPE=release
$ make -j$(nproc)
```

## Run SLOG on a single machine

The following command starts SLOG using the example configuration for a single-node cluster.
```
$ build/slog -config examples/single.conf -address /tmp/slog 
```

After that, use the client to send a transaction that writes some data.
```
$ build/client txn examples/write.json
...
Transaction ID: 100
Status: COMMITTED
Key set:
[WRITE] 0
        Value: aaaaaaaaaa
        New value: Hello
        Metadata: (0, 0)
[WRITE] 1
        Value: aaaaaaaaaa
        New value: World
        Metadata: (0, 0)
[WRITE] 2
        Value: aaaaaaaaaa
        New value: !!!!!
        Metadata: (0, 0)
Type: SINGLE_HOME
Code:
SET 0 Hello
SET 1 World
SET 2 !!!!!
Coordinating server: 0
Involved partitions: 0
Involved replicas: 0
```

Send a transaction to copy data from the previous keys to different keys:
```
$ build/client txn examples/copy.json
...
Transaction ID: 200
Status: COMMITTED
Key set:
[WRITE] 3
        Value: aaaaaaaaaa
        New value: Hello
        Metadata: (0, 0)
[WRITE] 4
        Value: aaaaaaaaaa
        New value: World
        Metadata: (0, 0)
[WRITE] 5
        Value: aaaaaaaaaa
        New value: !!!!!
        Metadata: (0, 0)
[READ] 0
        Value: Hello
        Metadata: (0, 0)
[READ] 1
        Value: World
        Metadata: (0, 0)
[READ] 2
        Value: !!!!!
        Metadata: (0, 0)
Type: SINGLE_HOME
Code:
COPY 0 3
COPY 1 4
COPY 2 5
Coordinating server: 0
Involved partitions: 0
Involved replicas: 0
```

Send a transaction to read the written data.
```
$ build/client txn examples/read.json
...
Transaction ID: 300
Status: COMMITTED
Key set:
[READ] 0
        Value: Hello
        Metadata: (0, 0)
[READ] 1
        Value: World
        Metadata: (0, 0)
[READ] 2
        Value: !!!!!
        Metadata: (0, 0)
[READ] 3
        Value: Hello
        Metadata: (0, 0)
[READ] 4
        Value: World
        Metadata: (0, 0)
[READ] 5
        Value: !!!!!
        Metadata: (0, 0)
Type: SINGLE_HOME
Code:
GET 0
GET 1
GET 2
GET 3
GET 4
GET 5
Coordinating server: 0
Involved partitions: 0
Involved replicas: 0
```

## Run SLOG on a cluster

The following guide shows how to manually run SLOG on a cluster of multiple machines. This can be time-consuming when the number of machines is large so you should use the [Admin tool](https://github.com/ctring/SLOG/wiki/Using-the-Admin-tool) instead.

In this example, we start SLOG on a cluster using the configuration in `examples/cluster.conf`. You need to change the IP addresses in this file to match with the addresses of your machines. You can add more machines by increasing either the number of replicas or the number of partitions in a replica. The number of machines in a replica must be the same across all replicas and equal to `num_partitions`.

After cloning and building SLOG, run the following command on each machine.
```
$ build/slog -config examples/cluster.conf -address <ip-address> -replica <replica-id> -partition <partition-id>
```

For example, assuming the machine configuration is
```
replicas: {
    addresses: "192.168.2.11",
    addresses: "192.168.2.12",
}
replicas: {
    addresses: "192.168.2.13",
    addresses: "192.168.2.14",
}
```

The commands to be run for the machines respectively from top to bottom are:
```
$ build/slog -config examples/cluster.conf -address 192.168.2.11 
``` 

```
$ build/slog -config examples/cluster.conf -address 192.168.2.12 
``` 

```
$ build/slog -config examples/cluster.conf -address 192.168.2.13 
``` 

```
$ build/slog -config examples/cluster.conf -address 192.168.2.14
```

Use the client to send a write transaction to a machine in the cluster. If you changed the `port` option in the configuration file, you need to use the `--port` argument in the command to match with the new port.
```
$ build/client txn examples/write.json --host 192.168.2.11
...
Transaction ID: 100
Status: COMMITTED
Key set:
[WRITE] 1
        Value: aaaaaaaaaa
        New value: World
        Metadata: (0, 0)
[WRITE] 0
        Value: aaaaaaaaaa
        New value: Hello
        Metadata: (0, 0)
[WRITE] 2
        Value: aaaaaaaaaa
        New value: !!!!!
        Metadata: (1, 0)
Type: MULTI_HOME_OR_LOCK_ONLY
Code:
SET 0 Hello
SET 1 World
SET 2 !!!!!
Coordinating server: 0
Involved partitions: 0 1
Involved replicas: 0 1
```

Send a copy transaction that copies the values from the written keys to new keys.
```
$ build/client txn examples/copy.json --host 192.168.2.11
...
Transaction ID: 200
Status: COMMITTED
Key set:
[WRITE] 3
        Value: aaaaaaaaaa
        New value: Hello
        Metadata: (1, 0)
[WRITE] 5
        Value: aaaaaaaaaa
        New value: !!!!!
        Metadata: (0, 0)
[READ] 1
        Value: World
        Metadata: (0, 0)
[WRITE] 4
        Value: aaaaaaaaaa
        New value: World
        Metadata: (0, 0)
[READ] 0
        Value: Hello
        Metadata: (0, 0)
[READ] 2
        Value: !!!!!
        Metadata: (1, 0)
Type: MULTI_HOME_OR_LOCK_ONLY
Code:
COPY 0 3
COPY 1 4
COPY 2 5
Coordinating server: 0
Involved partitions: 0 1
Involved replicas: 0 1
```

Send a read transaction to read the written data. This time, we read from a different replica to demonstrate that the data has been replicated.
```
$ build/client txn examples/read.json --host 192.168.2.13
...
Transaction ID: 102
Status: COMMITTED
Key set:
[READ] 0
        Value: Hello
        Metadata: (0, 0)
[READ] 2
        Value: !!!!!
        Metadata: (1, 0)
[READ] 4
        Value: World
        Metadata: (0, 0)
[READ] 1
        Value: World
        Metadata: (0, 0)
[READ] 3
        Value: Hello
        Metadata: (1, 0)
[READ] 5
        Value: !!!!!
        Metadata: (0, 0)
Type: MULTI_HOME_OR_LOCK_ONLY
Code:
GET 0
GET 1
GET 2
GET 3
GET 4
GET 5
Coordinating server: 2
Involved partitions: 0 1
Involved replicas: 0 1
```
