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

Upgrades from versions without reader leases must be coordinated: drain the old
server instances before enabling the new ones. Old binaries do not honor part
reader leases, so running old and new cleanup workers concurrently is unsafe.

### Cleanup

| Variable                        | Default | Description                                                           |
|---------------------------------|---------|-----------------------------------------------------------------------|
| `DISABLE_CLEANUP_JOBS`          | `false` | Disables background cleanup jobs.                                     |
| `CACHE_CLEANUP_OLDER_THAN_DAYS` | `90`    | Deletes cache storage locations not downloaded within this many days. |

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
