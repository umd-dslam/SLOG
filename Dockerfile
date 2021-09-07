FROM ubuntu:focal AS builder
    ARG CMAKE_OPTIONS

    # Avoid interactive installation
    ENV DEBIAN_FRONTEND=noninteractive 
    RUN apt-get update
    RUN apt-get -y install wget build-essential cmake git pkg-config

    WORKDIR /src

    COPY ./install-deps.sh .
    RUN ./install-deps.sh -d

    COPY . .
    RUN rm -rf build \
        && mkdir build \
        && cd build \
        && cmake .. -DBUILD_SLOG_TESTS=OFF ${CMAKE_OPTIONS} -DCMAKE_BUILD_TYPE=release \
        && make -j$(nproc) \
        && cd ..

FROM ubuntu:focal AS runner
    # If set (to anything), also create an image with tools (exclude the toolings)
    ARG INCLUDE_TOOLS

    WORKDIR /opt/slog
    COPY --from=builder /src/build/slog .
    COPY --from=builder /src/build/client .
    COPY --from=builder /src/build/benchmark .
    COPY --from=builder /src/build/scheduler_benchmark .
    COPY --from=builder /src/examples/*.conf ./
    COPY --from=builder /src/tools/ tools/

    RUN if [ -n "$INCLUDE_TOOLS" ]; then \
        apt-get update; \
        apt-get -y install python3 python3-pip; \
        python3 -m pip install -r tools/requirements.txt; \
        chmod +x tools/*.py; \
        fi

    ENV PATH="/opt/slog:${PATH}"
    ENV PATH="/opt/slog/tools:${PATH}"