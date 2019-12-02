# SLOG: Serializable, Low-latency, Geo-replicated Transactions

## How to Build

```
mkdir build
cd build
cmake ..
make -j
```

## How to Run Tests and Start a Server
To run the server, from the 'build' folder
```
./slog_server
```

To run tests, from the 'build' folder, run
```
ctest
```