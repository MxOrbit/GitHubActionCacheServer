# Performance Benchmark

Headline numbers are in [README.md](README.md#performance-benchmark). This file
holds the detailed tables and the methodology.

## Payload Size

Each cell is aggregate `MiB/s / p95 whole-cache latency`. Concurrency is one;
the upload of an individual cache can still use up to four block PUTs.

| Operation        |         Node v9.6.2 |             Go v1.0.5 |               Go v1.0.6 |
|------------------|--------------------:|----------------------:|------------------------:|
| Upload 1 MiB     |   52.69 / 18.545 ms | 60.65 / **13.485 ms** |   **60.89** / 13.597 ms |
| Download 1 MiB   |   26.91 / 41.848 ms |     122.95 / 9.015 ms |   **289.39 / 3.579 ms** |
| Upload 32 MiB    |  336.56 / 96.029 ms |    451.21 / 64.312 ms |  **692.34 / 39.049 ms** |
| Download 32 MiB  | 236.41 / 140.728 ms |    742.80 / 44.818 ms | **2684.56 / 12.395 ms** |
| Upload 128 MiB   | 385.31 / 313.966 ms |   510.43 / 229.796 ms | **960.00 / 106.543 ms** |
| Download 128 MiB | 273.52 / 502.549 ms |   879.12 / 155.499 ms | **3497.27 / 37.896 ms** |

## Concurrency Scaling

Each cache is 16 MiB. Cells are aggregate `upload MiB/s / download MiB/s`.

| Concurrent caches |     Node v9.6.2 |        Go v1.0.5 |            Go v1.0.6 |
|------------------:|----------------:|-----------------:|---------------------:|
|                 1 | 336.58 / 198.02 |  415.31 / 569.65 | **629.30 / 2129.78** |
|                 8 | 424.54 / 195.50 | 551.64 / 1594.68 | **798.00 / 3134.69** |
|                32 | 353.92 / 197.00 | 565.71 / 1352.89 | **791.28 / 2776.57** |

At concurrency 32, each direction transfers 1 GiB of logical payload.

| Concurrency-32 resource metric   | Node v9.6.2 |   Go v1.0.5 |       Go v1.0.6 |
|----------------------------------|------------:|------------:|----------------:|
| Upload p95 whole-cache latency   |  2341.32 ms |  1115.69 ms |   **820.37 ms** |
| Download p95 whole-cache latency |  2766.65 ms |   510.78 ms |   **306.31 ms** |
| Upload server CPU                |  1.23 cores |  0.85 cores |  **0.73 cores** |
| Download server CPU              |  2.96 cores |  2.39 cores |   **1.00 core** |
| Server write I/O during upload   | 1047.18 MiB | 2070.70 MiB | **1043.97 MiB** |
| Server write I/O during download | 1035.75 MiB | 1028.51 MiB |    **1.52 MiB** |

## Startup and Memory

Deployment payload is uncompressed. The Node value includes the 9.71 MiB
application output and the 123.03 MiB Node executable, but not shared OS
libraries; each Go value is one self-contained executable.

| Metric                            |         Node v9.6.2 |            Go v1.0.5 |        Go v1.0.6 |
|-----------------------------------|--------------------:|---------------------:|-----------------:|
| Deployment payload                |        >=132.74 MiB |            43.87 MiB |        44.32 MiB |
| Cold start p50 / p95              | 950.68 / 1298.00 ms | **10.49 / 33.01 ms** | 21.41 / 34.09 ms |
| Idle process count                |                   2 |                **1** |            **1** |
| Idle RSS p50                      |          399.68 MiB |        **26.28 MiB** |        28.41 MiB |
| Peak RSS, 32 concurrent transfers |          820.52 MiB |        **48.89 MiB** |        49.64 MiB |

## HTTP and Metadata

The health test has no database or storage work. CPU is expressed as average
fully utilized cores. Node uses upstream's shipped and recommended
`NITRO_CLUSTER_WORKERS=1` topology.

| Health metric (three-run median) | Node v9.6.2 |     Go v1.0.5 |    Go v1.0.6 |
|----------------------------------|------------:|--------------:|-------------:|
| 1 connection, requests/s         |      17,792 |        20,475 |   **20,737** |
| 1 connection, p99 latency        |    2.530 ms |      0.177 ms | **0.173 ms** |
| 64 connections, requests/s       |      38,140 |       313,255 |  **323,076** |
| 64 connections, p99 latency      |    4.800 ms |      1.310 ms | **1.240 ms** |
| 64 connections, server CPU       |  1.09 cores |    9.74 cores |   9.69 cores |
| 64 connections, requests/s/core  |  **34,943** |        32,156 |       33,340 |
| 64 connections, peak RSS         |  631.53 MiB | **46.89 MiB** |    48.03 MiB |

Metadata cells are `requests/s / p95 latency`.

| Cache v2 metadata operation |          Node v9.6.2 |            Go v1.0.5 |            Go v1.0.6 |
|-----------------------------|---------------------:|---------------------:|---------------------:|
| Hit, concurrency 1          |     1,947 / 0.663 ms |     2,821 / 0.470 ms | **3,040 / 0.416 ms** |
| Miss, concurrency 1         | **2,302** / 0.550 ms |     2,293 / 0.620 ms | 2,250 / **0.542 ms** |
| Hit, concurrency 32         |     5,666 / 7.822 ms | **6,520 / 6.558 ms** |     6,359 / 7.231 ms |
| Miss, concurrency 32        | **7,093 / 6.786 ms** |     6,215 / 7.215 ms |     6,009 / 7.714 ms |

## Test Setup

| Item            | Value                                                                                  |
|-----------------|----------------------------------------------------------------------------------------|
| Date            | 2026-07-29                                                                             |
| Host            | Intel Core i7-14700K, 28 logical CPUs exposed to WSL, 47.04 GiB available to WSL       |
| Host OS         | Windows build 10.0.26200.0                                                             |
| Guest           | Ubuntu 24.04.1 LTS on WSL2, kernel 6.18.33.2, ext4 virtual disk                        |
| Common backend  | SQLite plus local filesystem storage on the WSL ext4 disk                              |
| Network         | HTTP loopback (`127.0.0.1`), no TLS                                                    |
| Server topology | One service instance; `NITRO_CLUSTER_WORKERS=1` for Node                               |
| Data protocol   | Cache v2 JSON/Twirp, 4 MiB blocks, up to 4 parallel block PUTs per cache               |
| Server settings | Cleanup disabled, token signature validation skipped, stdout sent to `/dev/null`       |
| Toolchains      | Node 25.9.0 / pnpm 11.11.0; Go 1.26.5 with `CGO_ENABLED=0` and production linker flags |
| Load tools      | wrk 4.1.0; Python 3.12.3 / aiohttp 3.9.1                                               |

Each implementation was built from a separate clean clone:
[`f892a36`](https://github.com/falcondev-oss/github-actions-cache-server/commit/f892a367d1e6603e3828e3e2d754616fa8c2bf9e)
(Node v9.6.2),
[`0460bf4`](https://github.com/MxOrbit/GitHubActionCacheServer/commit/0460bf439c976c62c587f29c0406697b6d0b5103)
(v1.0.5), and
[`49e0c41`](https://github.com/MxOrbit/GitHubActionCacheServer/commit/49e0c41bac71a67193cf642610575eebc608dd30)
(v1.0.6). Each Go tag's Ent client was generated from its own tagged schema.

Startup uses five fresh-database runs. Health results are three-run medians.
Metadata uses 1,000 requests at concurrency 1 and 5,000 at concurrency 32. The
size sweep uses 9, 5, and 3 caches at 1, 32, and 128 MiB. The concurrency sweep
uses 8, 24, and 64 caches of 16 MiB at concurrency 1, 8, and 32. All groups
completed with zero HTTP, size, or integrity errors.

These local loopback, warm-page-cache results isolate server overhead; they are
not forecasts for a real network or disk. The matrix excludes PostgreSQL,
MySQL, S3, GCS, TLS, signature verification, cleanup contention, multiple
replicas, and end-to-end archive compression.
