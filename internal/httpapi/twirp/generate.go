// Package twirp contains the protobuf wire messages used by the GitHub Actions
// CacheService Twirp endpoints.
package twirp

//go:generate protoc --go_out=. --go_opt=paths=source_relative cache_service.proto
