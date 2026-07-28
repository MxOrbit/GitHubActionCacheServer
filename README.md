# GitHubActionCacheServer

[![License](https://img.shields.io/github/license/MxOrbit/GitHubActionCacheServer)](https://github.com/MxOrbit/GitHubActionCacheServer/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/MxOrbit/GitHubActionCacheServer?color=blueviolet&include_prereleases)](https://github.com/MxOrbit/GitHubActionCacheServer/releases)
[![Dockerhub](https://img.shields.io/docker/pulls/mmx233/action-cache-server)](https://hub.docker.com/r/mmx233/action-cache-server)

A Go implementation of the GitHub Actions cache service protocol for self-hosted
runners. It implements the cache v2 API surface used by `actions/cache`, stores
cache metadata in SQL, and stores cache objects on a local filesystem or S3.

The goal is to address performance bottlenecks while staying compatible with
[falcondev-oss/github-actions-cache-server](https://github.com/falcondev-oss/github-actions-cache-server)
by default. The only intentional compatibility gap is GCS storage support.

## Quick Start

Run locally with SQLite and filesystem storage:

```sh
go mod download
API_BASE_URL=http://localhost:3000 go run ./cmd/server
```

The server listens on `:3000` by default. Health checks:

```sh
curl http://localhost:3000/
curl http://localhost:3000/health
```

For local protocol tests only, `SKIP_TOKEN_VALIDATION=true` can be used to accept
unsigned or test JWTs. Do not use it for production.

## Docker

Image: `mmx233/action-cache-server`

Build the image:

```sh
docker build -t mmx233/action-cache-server .
```

Run a disposable local instance:

```sh
docker run --rm -p 3000:3000 \
  -e API_BASE_URL=http://localhost:3000 \
  -e DB_SQLITE_PATH=/tmp/cache-server/sqlite.db \
  -e STORAGE_FILESYSTEM_PATH=/tmp/cache-server/filesystem \
  mmx233/action-cache-server:latest
```

For persistent deployments, mount a writable data directory and point
`DB_SQLITE_PATH` and `STORAGE_FILESYSTEM_PATH` at that directory, or use an
external SQL database plus S3 storage.

## Configuration

### Server

| Variable                      | Default                                                  | Description                                                           |
|-------------------------------|----------------------------------------------------------|-----------------------------------------------------------------------|
| `ADDR`                        | `:3000`                                                  | HTTP listen address.                                                  |
| `API_BASE_URL`                | empty                                                    | Public base URL used when generating signed upload and download URLs. |
| `DEFAULT_ACTIONS_RESULTS_URL` | `https://results-receiver.actions.githubusercontent.com` | Upstream target for fallback proxy requests.                          |
| `DEBUG`                       | `false`                                                  | Enables debug log level.                                              |

### Authentication

| Variable                        | Default                                       | Description                                                                            |
|---------------------------------|-----------------------------------------------|----------------------------------------------------------------------------------------|
| `ACTIONS_TOKEN_ISSUER`          | `https://token.actions.githubusercontent.com` | Expected JWT issuer. Set this to the GHES Actions token issuer for GHES deployments.   |
| `GITHUB_ACTIONS_TOKEN_ISSUER`   | empty                                         | Legacy alias for `ACTIONS_TOKEN_ISSUER`; ignored when the canonical variable is set.   |
| `GITHUB_ACTIONS_TOKEN_JWKS_URL` | derived from the effective issuer             | Explicit JWKS URL override for deployments whose keys use a nonstandard URL.           |
| `SKIP_TOKEN_VALIDATION`         | `false`                                       | Parses JWTs without signature verification. Intended for tests only.                   |

Unless explicitly overridden, the JWKS URL is
`{effective issuer}/.well-known/jwks`. Trailing slashes are removed from the
effective issuer before JWT validation and JWKS derivation.

The token must contain `repository_id` and an `ac` claim with cache scopes. A
scope with permission `>= 2` is required for saves; scopes with permission `>= 1`
are used for restores.

### Database

| Variable               | Default           | Description                                           |
|------------------------|-------------------|-------------------------------------------------------|
| `DB_DRIVER`            | `sqlite`          | `sqlite`, `postgres`, or `mysql`.                     |
| `DB_SQLITE_PATH`       | `.data/sqlite.db` | SQLite database path.                                 |
| `DB_POSTGRES_URL`      | empty             | Full PostgreSQL DSN.                                  |
| `DB_POSTGRES_DATABASE` | empty             | PostgreSQL database when not using `DB_POSTGRES_URL`. |
| `DB_POSTGRES_HOST`     | empty             | PostgreSQL host when not using `DB_POSTGRES_URL`.     |
| `DB_POSTGRES_PORT`     | `5432`            | PostgreSQL port.                                      |
| `DB_POSTGRES_USER`     | empty             | PostgreSQL user.                                      |
| `DB_POSTGRES_PASSWORD` | empty             | PostgreSQL password.                                  |
| `DB_MYSQL_DATABASE`    | empty             | MySQL database.                                       |
| `DB_MYSQL_HOST`        | empty             | MySQL host.                                           |
| `DB_MYSQL_PORT`        | `3306`            | MySQL port.                                           |
| `DB_MYSQL_USER`        | empty             | MySQL user.                                           |
| `DB_MYSQL_PASSWORD`    | empty             | MySQL password.                                       |

Schema migrations run automatically at startup.

### Storage

| Variable                             | Default                    | Description                                                                                                     |
|--------------------------------------|----------------------------|-----------------------------------------------------------------------------------------------------------------|
| `STORAGE_DRIVER`                     | `filesystem`               | `filesystem` or `s3`.                                                                                           |
| `STORAGE_FILESYSTEM_PATH`            | `.data/storage/filesystem` | Root directory for filesystem storage.                                                                          |
| `STORAGE_S3_BUCKET`                  | empty                      | S3 bucket name. Required for S3 storage.                                                                        |
| `AWS_REGION`                         | `us-east-1`                | S3 region.                                                                                                      |
| `AWS_ENDPOINT_URL`                   | empty                      | Custom S3-compatible endpoint, such as MinIO.                                                                   |
| `STORAGE_S3_FORCE_PATH_STYLE`        | `true`                     | Uses path-style S3 addressing.                                                                                  |
| `STORAGE_S3_KEY_PREFIX`              | `gh-actions-cache`         | Prefix for all S3 keys.                                                                                         |
| `STORAGE_S3_UPLOAD_PART_SIZE_BYTES`  | `5242880`                  | S3 upload part size and threshold in integer bytes. Minimum `5242880` bytes (5 MiB); lower values fail startup. |
| `STORAGE_S3_UPLOAD_CONCURRENCY`      | `1`                        | S3 transfer manager upload worker count per object.                                                             |
| `STORAGE_S3_MULTIPART_ABORT_TIMEOUT` | `30s`                      | Timeout for aborting failed S3 multipart uploads.                                                               |

S3 credentials are loaded through the AWS SDK default credential chain.

For S3 deployments, configure an `AbortIncompleteMultipartUpload` bucket
lifecycle rule. It cleans up multipart uploads left incomplete if the server
exits during composition; verify lifecycle support when using an S3-compatible
endpoint.

An AWS S3 lifecycle configuration can scope the rule to the cache prefix:

```json
{
  "Rules": [
    {
      "ID": "abort-incomplete-cache-uploads",
      "Status": "Enabled",
      "Filter": { "Prefix": "gh-actions-cache/" },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 1 }
    }
  ]
}
```

### Cache Behavior

| Variable                      | Default              | Description                                                                                                    |
|-------------------------------|----------------------|----------------------------------------------------------------------------------------------------------------|
| `ENABLE_DIRECT_DOWNLOADS`     | `false`              | When using S3, presigns eligible cache objects instead of proxying their downloads through the server.         |
| `DOWNLOAD_URL_SIGNING_SECRET` | generated at startup | HMAC secret for local signed download URLs. Set a stable value for multi-instance or restart-safe deployments. |
| `CACHE_MERGE_CONCURRENCY`     | CPU count            | Maximum number of concurrent S3 materializations. Values below `1` fall back to CPU count.                     |

Local signed download URLs and S3 direct download URLs expire after 10 minutes.
Single-part S3 caches are presigned directly. Multi-part caches are presigned
after composition when all non-final parts satisfy the backend's multipart
upload size constraints (5 MiB on AWS S3); unsupported layouts transparently
fall back to server-proxied downloads.

Active downloads are protected by durable database reader leases. Proxied
downloads renew a two-minute lease every 30 seconds and release it when the
response closes. S3 direct-download leases last for the full signed URL TTL.
Cache deletion first detaches the cache entry and fences its storage location;
physical deletion waits for reader and materialization leases and for a
10-minute compatibility grace period. This also protects direct URLs issued
immediately before an upgrade to the lease-aware schema.

Before returning either a local or S3 download URL, the server checks the
selected representation's anchor object with one filesystem stat or S3 HEAD:
`merged` for a completed materialization and `parts/0` otherwise. A confirmed
missing object detaches the dangling cache entry through the same fenced
deletion path and retries matching, allowing another restore key or scope to
win. Storage probe errors preserve metadata and fail the lookup. This constant-
cost check intentionally does not scan every part, so isolated external
deletion of an interior part is detected only when that part is downloaded.

Upgrades from versions without reader leases must be coordinated: drain the old
server instances before enabling the new ones. Old binaries do not honor part
reader leases, so running old and new cleanup workers concurrently is unsafe.

### Cleanup

| Variable                        | Default | Description                                                                                  |
|---------------------------------|---------|----------------------------------------------------------------------------------------------|
| `DISABLE_CLEANUP_JOBS`          | `false` | Disables background cleanup jobs.                                                            |
| `CACHE_CLEANUP_OLDER_THAN_DAYS` | `90`    | Deletes inactive cache storage locations after this many days. Set to `0` to disable expiry. |

A location expires only when its last download and every referencing entry's
save time predate the cutoff. Active reader leases defer expiry, and proxied or
direct-URL access refreshes recency at most once every 10 minutes.

Cleanup intervals:

- abandoned uploads: every 5 minutes
- pending physical storage deletions: every 5 minutes
- fenced storage locations waiting for leases or the deletion grace period: every 5 minutes
- expired reader leases: every hour
- expired cache entries: every 24 hours
- orphan storage locations: every 24 hours
- superseded parts: every hour, once the merged representation is at least one hour old

Physical folder deletion uses a transactional database outbox after the
location's deletion fence is ready. A failed or interrupted filesystem/S3
deletion remains queued and is retried by the cleanup worker with exponential
backoff from 5 minutes up to 24 hours. HTTP request paths only change database
state, and database transactions are never held open across storage I/O.

### Management API

| Variable             | Default | Description                                                                     |
|----------------------|---------|---------------------------------------------------------------------------------|
| `MANAGEMENT_API_KEY` | empty   | Enables management endpoints when set. Protected endpoints require `X-Api-Key`. |

Endpoints:

- `GET /management-api/_docs`
- `GET /management-api/_docs/spec.json`
- `GET /management-api/cache-entries/`
- `DELETE /management-api/cache-entries/`
- `GET /management-api/cache-entries/match`
- `GET /management-api/cache-entries/:id`
- `DELETE /management-api/cache-entries/:id`
- `GET /management-api/storage-locations/:id`
- `DELETE /management-api/storage-locations/:id`
- `POST /management-api/_rpc`
- `POST /management-api/_rpc/*procedure`

## Development

```sh
go mod download
go test ./...
```

External integration coverage can be run when the required services are
available:

```sh
E2E_POSTGRES_URL='postgres://cache:cache@127.0.0.1:5432/cache_test?sslmode=disable' \
E2E_MYSQL_HOST=127.0.0.1 \
E2E_MYSQL_DATABASE=cache_test \
E2E_MYSQL_USER=cache \
E2E_S3_ENDPOINT_URL=http://127.0.0.1:9000 \
E2E_S3_BUCKET=cache-test \
go test ./e2e -run 'TestExternal(Postgres|MySQL|S3MinIO)SaveAndRestore' -count=1
```

Targeted data-path benchmarks cover filesystem whole-cache upload/download,
Azure block commit, ordered-parts versus merged downloads, concurrent runners,
and S3 server-side composition:

```sh
go test ./internal/cache ./internal/storage -run '^$' -bench 'Benchmark(Filesystem|Azure|S3)' -benchmem -count=5
```

Streaming benchmarks run with 32 KiB, 128 KiB, 256 KiB, and 1 MiB buffers.
The concurrent-runner benchmark also reports p95 latency and peak process RSS;
the deterministic S3 protocol benchmark reports and asserts SDK HTTP requests
per composition. When `E2E_S3_ENDPOINT_URL` and `E2E_S3_BUCKET` are configured,
the same command also runs composition against that external S3-compatible
backend to measure its wall-clock latency.

The repository also contains a Go smoke test for the cache v2 HTTP protocol
shape. Full runner-level compatibility should be tested with the patched
falcondev runner container and a real GitHub Actions job.

## Performance Benchmark

On this local WSL2 filesystem benchmark, Go v1.0.6 is **2.24x faster for
upload and 14.09x faster for download** than upstream Node v9.6.2 at 32
concurrent caches. It also starts **44x faster** and uses **93% less idle
memory**. Against Go v1.0.5, the same concurrent workload improves upload by
1.40x and download by 2.05x.

| Headline result | Node v9.6.2 | Go v1.0.5 | Go v1.0.6 |
|---|---:|---:|---:|
| 32-cache upload | 353.92 MiB/s | 565.71 MiB/s | **791.28 MiB/s** |
| 32-cache download | 197.00 MiB/s | 1352.89 MiB/s | **2776.57 MiB/s** |
| 128 MiB upload | 385.31 MiB/s | 510.43 MiB/s | **960.00 MiB/s** |
| 128 MiB download | 273.52 MiB/s | 879.12 MiB/s | **3497.27 MiB/s** |
| Cold start p50 | 950.68 ms | **10.49 ms** | 21.41 ms |
| Idle RSS | 399.68 MiB | **26.28 MiB** | 28.41 MiB |
| Server writes during 1 GiB download | 1035.75 MiB | 1028.51 MiB | **1.52 MiB** |

The main exception is metadata miss throughput: at concurrency 32, Node leads
with 7,093 requests/s versus 6,215 for v1.0.5 and 6,009 for v1.0.6.

### Payload Size

Each cell is aggregate `MiB/s / p95 whole-cache latency`. Concurrency is one;
the upload of an individual cache can still use up to four block PUTs.

| Operation | Node v9.6.2 | Go v1.0.5 | Go v1.0.6 |
|---|---:|---:|---:|
| Upload 1 MiB | 52.69 / 18.545 ms | 60.65 / **13.485 ms** | **60.89** / 13.597 ms |
| Download 1 MiB | 26.91 / 41.848 ms | 122.95 / 9.015 ms | **289.39 / 3.579 ms** |
| Upload 32 MiB | 336.56 / 96.029 ms | 451.21 / 64.312 ms | **692.34 / 39.049 ms** |
| Download 32 MiB | 236.41 / 140.728 ms | 742.80 / 44.818 ms | **2684.56 / 12.395 ms** |
| Upload 128 MiB | 385.31 / 313.966 ms | 510.43 / 229.796 ms | **960.00 / 106.543 ms** |
| Download 128 MiB | 273.52 / 502.549 ms | 879.12 / 155.499 ms | **3497.27 / 37.896 ms** |

### Concurrency Scaling

Each cache is 16 MiB. Cells are aggregate `upload MiB/s / download MiB/s`.

| Concurrent caches | Node v9.6.2 | Go v1.0.5 | Go v1.0.6 |
|---:|---:|---:|---:|
| 1 | 336.58 / 198.02 | 415.31 / 569.65 | **629.30 / 2129.78** |
| 8 | 424.54 / 195.50 | 551.64 / 1594.68 | **798.00 / 3134.69** |
| 32 | 353.92 / 197.00 | 565.71 / 1352.89 | **791.28 / 2776.57** |

At concurrency 32, each direction transfers 1 GiB of logical payload.

| Concurrency-32 resource metric | Node v9.6.2 | Go v1.0.5 | Go v1.0.6 |
|---|---:|---:|---:|
| Upload p95 whole-cache latency | 2341.32 ms | 1115.69 ms | **820.37 ms** |
| Download p95 whole-cache latency | 2766.65 ms | 510.78 ms | **306.31 ms** |
| Upload server CPU | 1.23 cores | 0.85 cores | **0.73 cores** |
| Download server CPU | 2.96 cores | 2.39 cores | **1.00 core** |
| Server write I/O during upload | 1047.18 MiB | 2070.70 MiB | **1043.97 MiB** |
| Server write I/O during download | 1035.75 MiB | 1028.51 MiB | **1.52 MiB** |

### Startup and Memory

Deployment payload is uncompressed. The Node value includes the 9.71 MiB
application output and the 123.03 MiB Node executable, but not shared OS
libraries; each Go value is one self-contained executable.

| Metric | Node v9.6.2 | Go v1.0.5 | Go v1.0.6 |
|---|---:|---:|---:|
| Deployment payload | >=132.74 MiB | 43.87 MiB | 44.32 MiB |
| Cold start p50 / p95 | 950.68 / 1298.00 ms | **10.49 / 33.01 ms** | 21.41 / 34.09 ms |
| Idle process count | 2 | **1** | **1** |
| Idle RSS p50 | 399.68 MiB | **26.28 MiB** | 28.41 MiB |
| Peak RSS, 32 concurrent transfers | 820.52 MiB | **48.89 MiB** | 49.64 MiB |

### HTTP and Metadata

The health test has no database or storage work. CPU is expressed as average
fully utilized cores. Node uses upstream's shipped and recommended
`NITRO_CLUSTER_WORKERS=1` topology.

| Health metric (three-run median) | Node v9.6.2 | Go v1.0.5 | Go v1.0.6 |
|---|---:|---:|---:|
| 1 connection, requests/s | 17,792 | 20,475 | **20,737** |
| 1 connection, p99 latency | 2.530 ms | 0.177 ms | **0.173 ms** |
| 64 connections, requests/s | 38,140 | 313,255 | **323,076** |
| 64 connections, p99 latency | 4.800 ms | 1.310 ms | **1.240 ms** |
| 64 connections, server CPU | 1.09 cores | 9.74 cores | 9.69 cores |
| 64 connections, requests/s/core | **34,943** | 32,156 | 33,340 |
| 64 connections, peak RSS | 631.53 MiB | **46.89 MiB** | 48.03 MiB |

Metadata cells are `requests/s / p95 latency`.

| Cache v2 metadata operation | Node v9.6.2 | Go v1.0.5 | Go v1.0.6 |
|---|---:|---:|---:|
| Hit, concurrency 1 | 1,947 / 0.663 ms | 2,821 / 0.470 ms | **3,040 / 0.416 ms** |
| Miss, concurrency 1 | **2,302** / 0.550 ms | 2,293 / 0.620 ms | 2,250 / **0.542 ms** |
| Hit, concurrency 32 | 5,666 / 7.822 ms | **6,520 / 6.558 ms** | 6,359 / 7.231 ms |
| Miss, concurrency 32 | **7,093 / 6.786 ms** | 6,215 / 7.215 ms | 6,009 / 7.714 ms |

### Test Setup

| Item | Value |
|---|---|
| Date | 2026-07-29 |
| Host | Intel Core i7-14700K, 28 logical CPUs exposed to WSL, 47.04 GiB available to WSL |
| Host OS | Windows build 10.0.26200.0 |
| Guest | Ubuntu 24.04.1 LTS on WSL2, kernel 6.18.33.2, ext4 virtual disk |
| Common backend | SQLite plus local filesystem storage on the WSL ext4 disk |
| Network | HTTP loopback (`127.0.0.1`), no TLS |
| Server topology | One service instance; `NITRO_CLUSTER_WORKERS=1` for Node |
| Data protocol | Cache v2 JSON/Twirp, 4 MiB blocks, up to 4 parallel block PUTs per cache |
| Server settings | Cleanup disabled, token signature validation skipped, stdout sent to `/dev/null` |
| Toolchains | Node 25.9.0 / pnpm 11.11.0; Go 1.26.5 with `CGO_ENABLED=0` and production linker flags |
| Load tools | wrk 4.1.0; Python 3.12.3 / aiohttp 3.9.1 |

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
