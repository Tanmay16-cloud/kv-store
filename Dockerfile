FROM ubuntu:24.04 AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential cmake && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY CMakeLists.txt .
COPY include include
COPY src src
COPY tests tests

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --config Release && \
    ctest --test-dir build --output-on-failure

FROM ubuntu:24.04 AS runtime

RUN useradd --create-home --home-dir /data --shell /usr/sbin/nologin kvstore && \
    chown -R kvstore:kvstore /data

COPY --from=build /src/build/kvstore /usr/local/bin/kvstore
COPY --from=build /src/build/kv-cli /usr/local/bin/kv-cli
COPY --from=build /src/build/kv-bench /usr/local/bin/kv-bench

WORKDIR /data
USER kvstore

EXPOSE 5000 5001 5002

CMD ["kvstore", "serve", "5000"]
