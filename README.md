# SLOG

[![Build Status](https://travis-ci.com/ctring/SLOG.svg?branch=master)](https://travis-ci.com/ctring/SLOG)

# What is SLOG?

SLOG is a geographically distributed data store that achieves high-throughput and low-latency transactions while guaranteeing strict serializability. 
For more details, see this [blog post](http://dbmsmusings.blogspot.com/2019/10/introducing-slog-cheating-low-latency.html) or the [SLOG paper](http://www.vldb.org/pvldb/vol12/p1747-ren.pdf).

This repository contains an experimental implementation of the system, which is not suitable for use in production.

# Getting Started 

The following guide has been tested on Ubuntu 20.04 with GCC 9.3.0 and CMake 3.16.3. Additional docs are in the Wiki.

## Build SLOG

Run the following commands to build the system. The dependencies will be downloaded and built automatically.

```
$ mkdir build
$ cd build
$ cmake ..
$ make -j4
```

## Run SLOG on a single machine

The following command starts SLOG using the example configuration for a single-node cluster.
```
$ build/slog -config examples/single.conf
```

After that, use the client to send a transaction that writes some data.
```
$ build/client txn examples/write_sh.1.json
```
This command will return the following confirmation.
```
Transaction ID: 1000
Status: COMMITTED
Read set:
Write set:
         A ==> Hello
         B ==> World
         C ==> !!!!!
Type: SINGLE_HOME
Code: SET A Hello SET B World SET C !!!!!
```

Send another transaction to read the written data.
```
$ build/client txn examples/read_sh.1.json
```
Below is the result of the read transaction. Note that this time, the key-value pairs are from the readset.
```
Transaction ID: 2000
Status: COMMITTED
Read set:
         A ==> Hello
         B ==> World
         C ==> !!!!!
Write set:
Type: SINGLE_HOME
Code: GET A GET B GET C
```


## Run SLOG on a cluster

The following guide shows how to manually run SLOG on a cluster of multiple machines. This can be time-consuming when the number of machines is large so you should use the [Admin tool](https://github.com/ctring/SLOG/wiki/Using-the-Admin-tool) instead.

In this example, we start SLOG on a cluster using the configuration in `examples/cluster.conf`. You need to change the IP addresses in this file to match with the addresses of your machines. You can add more machines by increasing either the number of replicas or the number of partitions in a replica. The number of machines in a replica must be the same across all replicas and equal to `num_partitions`.

After cloning and building SLOG, run the following command on each machine.
```
build/slog -config examples/cluster.conf -address <ip-address> -replica <replica-id> -partition <partition-id>
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
build/slog -config examples/cluster.conf -address 192.168.2.11 -replica 0 -partition 0
``` 

```
build/slog -config examples/cluster.conf -address 192.168.2.12 -replica 0 -partition 1
``` 

```
build/slog -config examples/cluster.conf -address 192.168.2.13 -replica 1 -partition 0
``` 

```
build/slog -config examples/cluster.conf -address 192.168.2.14 -replica 1 -partition 1
```

Use the client to send a write transaction to a machine in the cluster. If you changed the `port` option in the configuration file, you need to use the `--port` argument in the command to match with the new port.
```
build/client txn examples/write.1.json --host 192.168.2.11
```

The following confirmation is returned.

```
Transaction ID: 1000
Status: COMMITTED
Read set:
Write set:
         A ==> Hello
         B ==> World
         C ==> !!!!!
Type: SINGLE_HOME
Code: SET A Hello SET B World SET C !!!!!
```

Send a read transaction to read the written data. This time, we read from a different replica to demonstrate that the data has been replicated.
```
build/client txn examples/read.1.json --host 192.168.2.13
```
The result is
```
Transaction ID: 1002
Status: COMMITTED
Read set:
         A ==> Hello
         B ==> World
         C ==> !!!!!
Write set:
Type: SINGLE_HOME
Code: GET A GET B GET C
```
