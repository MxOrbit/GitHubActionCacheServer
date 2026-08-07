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

Download a prebuilt binary for your platform from
[Releases](https://github.com/MxOrbit/GitHubActionCacheServer/releases):

```sh
curl -LO https://github.com/MxOrbit/GitHubActionCacheServer/releases/latest/download/action-cache-server_linux_amd64.tar.gz
tar -xzf action-cache-server_linux_amd64.tar.gz
API_BASE_URL=http://localhost:3000 ./action-cache-server
```

A multi-arch image is also available on Docker Hub (see [Docker](#docker)):

```sh
docker run --rm -p 3000:3000 -e API_BASE_URL=http://localhost:3000 mmx233/action-cache-server:latest
```

Or run from source with SQLite and filesystem storage:

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

Image: `mmx233/action-cache-server` on Docker Hub, built for
`linux/amd64`, `linux/arm64`, `linux/arm/v7`, and `linux/arm/v6`. Tags are
`latest` and each release version (for example `v1.0.6`).

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

To build the image locally instead:

```sh
docker build -t mmx233/action-cache-server .
```

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

The token must contain `repository_id`, an `ac` claim with cache scopes, and a
valid `exp` claim. A scope with permission `>= 2` is required for saves; scopes
with permission `>= 1` are used for restores.

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
| `DB_POSTGRES_SSLMODE`  | `prefer`          | TLS mode for discrete PostgreSQL parameters.          |
| `DB_MYSQL_DATABASE`    | empty             | MySQL database.                                       |
| `DB_MYSQL_HOST`        | empty             | MySQL host.                                           |
| `DB_MYSQL_PORT`        | `3306`            | MySQL port.                                           |
| `DB_MYSQL_USER`        | empty             | MySQL user.                                           |
| `DB_MYSQL_PASSWORD`    | empty             | MySQL password.                                       |
| `DB_MYSQL_TLS`         | `preferred`       | MySQL TLS mode.                                       |

Schema migrations run automatically at startup and are serialized across instances.

`DB_POSTGRES_URL` sets its own TLS parameters and ignores
`DB_POSTGRES_SSLMODE`. The opportunistic defaults permit plaintext fallback; use
PostgreSQL `verify-full` or MySQL `true` with the CA in the system trust store
for authenticated TLS.

### Storage

| Variable                             | Default                    | Description                                                                                                     |
|--------------------------------------|----------------------------|-----------------------------------------------------------------------------------------------------------------|
| `STORAGE_DRIVER`                     | `filesystem`               | `filesystem` or `s3`.                                                                                           |
| `STORAGE_FILESYSTEM_PATH`            | `.data/storage/filesystem` | Root directory for filesystem storage.                                                                          |
| `STORAGE_FILESYSTEM_FSYNC`           | `true`                     | Sync filesystem upload contents before publishing them; disable only to trade crash durability for throughput.  |
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
lifecycle rule scoped to the key prefix (for example, `DaysAfterInitiation: 1`)
to clean up multipart uploads left incomplete if the server exits during
composition; verify lifecycle support when using an S3-compatible endpoint.

### Cache Behavior

| Variable                             | Default              | Description                                                                                                    |
|--------------------------------------|----------------------|----------------------------------------------------------------------------------------------------------------|
| `ENABLE_DIRECT_DOWNLOADS`            | `false`              | When using S3, presigns eligible cache objects instead of proxying their downloads through the server.         |
| `DOWNLOAD_URL_SIGNING_SECRET`        | generated at startup | HMAC secret for local signed download URLs. Set a stable value for multi-instance or restart-safe deployments. |
| `CACHE_MERGE_CONCURRENCY`            | CPU count            | Maximum number of concurrent S3 materializations. Values below `1` fall back to CPU count.                     |
| `CACHE_MAX_SIZE_BYTES`               | unset                | Maximum logical cache payload bytes for any backend. Must be a positive integer when set.                      |
| `CACHE_FILESYSTEM_MAX_USAGE_PERCENT` | `90`                 | Filesystem volume usage that triggers eviction when no explicit byte budget is set. Range `(0, 100]`.          |

Local signed download URLs and S3 direct download URLs expire after 10 minutes.
Eligible caches are presigned directly; layouts that do not satisfy the
backend's multipart constraints transparently fall back to server-proxied
downloads.

Capacity eviction runs after startup, every 10 minutes, and after finalize. It
removes least-recently-used entries to 90% of the active budget. Filesystem
usage includes non-cache data, in-progress uploads, and temporary files on the
same volume. S3 is unlimited unless an explicit byte budget is configured.

Active downloads are protected by database reader leases. Deletion detaches the
cache entry first and physically removes storage only after readers drain and a
10-minute grace period ends. Before returning a download URL, the server stats
the anchor object; a confirmed-missing object detaches the entry and retries
matching.

Upgrades from versions without reader leases must be coordinated: drain the old
server instances before enabling the new ones. Old binaries do not honor part
reader leases, so running old and new cleanup workers concurrently is unsafe.

### Cleanup

| Variable                              | Default | Description                                                                                  |
|---------------------------------------|---------|----------------------------------------------------------------------------------------------|
| `DISABLE_CLEANUP_JOBS`                | `false` | Disables background cleanup jobs.                                                            |
| `CACHE_CLEANUP_OLDER_THAN_DAYS`       | `90`    | Deletes inactive cache storage locations after 1–36500 days. Set to `0` to disable expiry.   |
| `ORPHANED_STORAGE_GRACE_PERIOD_HOURS` | `24`    | Retains unreferenced physical storage folders for at least this many hours.                  |

A location expires only when its last download and every referencing entry's
save time predate the cutoff. Active reader leases defer expiry, and proxied or
direct-URL access refreshes recency at most once every 10 minutes.

Cleanup jobs run on fixed intervals from 5 minutes to 24 hours. The database is
authoritative for storage reconciliation, so restoring an older database can
reclaim newer folders that it does not reference.
The daily filesystem scan also removes `.upload-*` temporary files older than
24 hours, while retaining every folder that still has an active upload record.

### Metrics

Unauthenticated Prometheus metrics are available at `GET /metrics`. The endpoint
exports Go and process metrics plus:

- `cache_requests_total{result="hit"|"miss"}`
- `cache_uploads_total`
- `cache_storage_bytes`
- `storage_deletions_pending`
- `storage_deletion_oldest_age_seconds`
- `storage_deletion_failures_total`

`cache_storage_bytes` sums known logical payload sizes for every tracked storage
location, including fenced rows until lifecycle finalization; it is not an exact
physical-storage reading. Scrapes query persisted metadata and never walk the
filesystem or list S3.

Request, upload, and deletion-failure counters are per process; aggregate their
rates across replicas with `sum`. The storage and outbox gauges read the shared
database and are repeated by every replica, so use `max` or select one target
instead of summing them.

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

The repository also contains a Go smoke test for the cache v2 HTTP protocol
shape. Full runner-level compatibility should be tested with the patched
falcondev runner container and a real GitHub Actions job.

## Performance Benchmark

On this local WSL2 filesystem benchmark, Go v1.0.6 is **2.24x faster for
upload and 14.09x faster for download** than upstream Node v9.6.2 at 32
concurrent caches. It also starts **44x faster** and uses **93% less idle
memory**. Against Go v1.0.5, the same concurrent workload improves upload by
1.40x and download by 2.05x.

| Headline result                     |  Node v9.6.2 |     Go v1.0.5 |         Go v1.0.6 |
|-------------------------------------|-------------:|--------------:|------------------:|
| 32-cache upload                     | 353.92 MiB/s |  565.71 MiB/s |  **791.28 MiB/s** |
| 32-cache download                   | 197.00 MiB/s | 1352.89 MiB/s | **2776.57 MiB/s** |
| 128 MiB upload                      | 385.31 MiB/s |  510.43 MiB/s |  **960.00 MiB/s** |
| 128 MiB download                    | 273.52 MiB/s |  879.12 MiB/s | **3497.27 MiB/s** |
| Cold start p50                      |    950.68 ms |  **10.49 ms** |          21.41 ms |
| Idle RSS                            |   399.68 MiB | **26.28 MiB** |         28.41 MiB |
| Server writes during 1 GiB download |  1035.75 MiB |   1028.51 MiB |      **1.52 MiB** |

The main exception is metadata miss throughput: at concurrency 32, Node leads
with 7,093 requests/s versus 6,215 for v1.0.5 and 6,009 for v1.0.6.

Detailed payload-size, concurrency-scaling, startup/memory, and HTTP/metadata
tables, plus the test setup and methodology, are in [BENCHMARK.md](BENCHMARK.md).
These local loopback, warm-page-cache results isolate server overhead; they are
not forecasts for a real network or disk.
