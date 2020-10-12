FROM ubuntu:bionic AS builder
    ARG CMAKE_OPTIONS

    RUN apt-get update
    RUN apt-get -y install wget

    WORKDIR /tmp
    RUN wget https://github.com/Kitware/CMake/releases/download/v3.12.4/cmake-3.12.4-Linux-x86_64.sh \
        -O cmake-install.sh \
        && chmod u+x cmake-install.sh \
        && ./cmake-install.sh --skip-license --prefix=/usr \
        && rm cmake-install.sh

    WORKDIR /src

    COPY install-deps.sh .
    RUN ./install-deps.sh --docker

    COPY . .
    RUN rm -rf build \
        && mkdir build \
        && cd build \
        && cmake .. -DCMAKE_BUILD_TYPE=release -DBUILD_TESTING=OFF ${CMAKE_OPTIONS}\
        && make -j$(nproc) \
        && cd ..

FROM ubuntu:bionic AS runner
    # If set (to anything), also create an image with tools (exclude the toolings)
    ARG INCLUDE_TOOLS

    WORKDIR /opt/slog
    COPY --from=builder /src/build/slog .
    COPY --from=builder /src/build/client .
    COPY --from=builder /src/build/benchmark .
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