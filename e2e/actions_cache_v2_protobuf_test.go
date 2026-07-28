package e2e

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

const protobufMediaType = "application/protobuf"

type protobufField struct {
	number protowire.Number
	typeID protowire.Type
	bytes  []byte
	value  uint64
}

func TestSccacheCompatibleProtobufSaveAndRestore(t *testing.T) {
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	router := newTestRouter(t)
	token := actionsToken(t)
	key := "sccache-protobuf-key"
	archive := "sccache protobuf archive"

	createRec := postProtobuf(
		t,
		router,
		createCacheEntryPath,
		token,
		protobufCreateCacheEntryRequest(protobufCacheMetadata(999, "untrusted-create-scope"), key, defaultCacheEntryVersion),
	)
	require.Equal(t, http.StatusOK, createRec.Code)
	require.Equal(t, protobufMediaType, createRec.Header().Get("Content-Type"))
	createFields := decodeProtobufFields(t, createRec.Body.Bytes())
	require.Equal(t, uint64(1), requireProtobufVarintField(t, createFields, 1))
	uploadURL := requireProtobufStringField(t, createFields, 2)
	uploadWholeCache(t, router, parseSignedURL(t, uploadURL), archive)

	finalizeRec := postProtobuf(
		t,
		router,
		finalizeCacheEntryPath,
		token,
		protobufFinalizeCacheEntryUploadRequest(
			protobufCacheMetadata(888, "untrusted-finalize-scope"),
			key,
			int64(len(archive)),
			defaultCacheEntryVersion,
		),
	)
	require.Equal(t, http.StatusOK, finalizeRec.Code)
	require.Equal(t, protobufMediaType, finalizeRec.Header().Get("Content-Type"))
	finalizeFields := decodeProtobufFields(t, finalizeRec.Body.Bytes())
	require.Equal(t, uint64(1), requireProtobufVarintField(t, finalizeFields, 1))
	require.Positive(t, requireProtobufVarintField(t, finalizeFields, 2))

	matchRec := postProtobuf(
		t,
		router,
		getCacheEntryDownloadPath,
		token,
		protobufGetCacheEntryDownloadURLRequest(
			protobufCacheMetadata(777, "untrusted-restore-scope"),
			"missing-primary-key",
			[]string{"sccache-"},
			defaultCacheEntryVersion,
		),
	)
	require.Equal(t, http.StatusOK, matchRec.Code)
	require.Equal(t, protobufMediaType, matchRec.Header().Get("Content-Type"))
	matchFields := decodeProtobufFields(t, matchRec.Body.Bytes())
	require.Equal(t, uint64(1), requireProtobufVarintField(t, matchFields, 1))
	downloadURL := requireProtobufStringField(t, matchFields, 2)
	require.Equal(t, key, requireProtobufStringField(t, matchFields, 3))
	require.Equal(t, archive, downloadCache(t, router, parseSignedURL(t, downloadURL)))
}

func TestSccacheCompatibleProtobufCacheMiss(t *testing.T) {
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	router := newTestRouter(t)

	rec := postProtobuf(
		t,
		router,
		getCacheEntryDownloadPath,
		actionsToken(t),
		protobufGetCacheEntryDownloadURLRequest(
			protobufCacheMetadata(123, "refs/heads/main"),
			"missing-key",
			nil,
			defaultCacheEntryVersion,
		),
	)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, protobufMediaType, rec.Header().Get("Content-Type"))
	_, ok := protobufVarintField(decodeProtobufFields(t, rec.Body.Bytes()), 1)
	require.False(t, ok, "proto3 false is encoded as the absent default value")
}

func TestProtobufLookupSelfHealsDanglingCacheEntryAsCleanMiss(t *testing.T) {
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	app := newTestApp(t)
	token := actionsToken(t)
	key := "dangling-protobuf-cache"
	createBody := cacheBody(key)
	uploadURL := createCacheEntry(t, app.router, token, createBody)
	uploadWholeCache(t, app.router, uploadURL, "data")
	finalizeCacheEntry(t, app.router, token, createBody)
	require.NoError(t, app.storage.Clear(context.Background()))

	rec := postProtobuf(
		t,
		app.router,
		getCacheEntryDownloadPath,
		token,
		protobufGetCacheEntryDownloadURLRequest(
			protobufCacheMetadata(123, "refs/heads/main"),
			key,
			nil,
			defaultCacheEntryVersion,
		),
	)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, protobufMediaType, rec.Header().Get("Content-Type"))
	_, ok := protobufVarintField(decodeProtobufFields(t, rec.Body.Bytes()), 1)
	require.False(t, ok)
	require.Zero(t, app.db.CacheEntry.Query().CountX(context.Background()))
}

func TestProtobufFinalizeMissingUploadReachesDomainHandling(t *testing.T) {
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	router := newTestRouter(t)

	rec := postProtobuf(
		t,
		router,
		finalizeCacheEntryPath,
		actionsToken(t),
		protobufFinalizeCacheEntryUploadRequest(
			protobufCacheMetadata(123, "refs/heads/main"),
			"never-uploaded",
			4096,
			defaultCacheEntryVersion,
		),
	)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.NotEqual(t, http.StatusBadRequest, rec.Code)
}

func TestMalformedProtobufCacheRequestReturnsBadRequest(t *testing.T) {
	t.Setenv("SKIP_TOKEN_VALIDATION", "true")
	router := newTestRouter(t)

	// Field 2 claims a five-byte string but contains only one byte.
	rec := postProtobuf(t, router, createCacheEntryPath, actionsToken(t), []byte{0x12, 0x05, 'x'})

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func postProtobuf(
	t *testing.T,
	router http.Handler,
	path string,
	token string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", protobufMediaType+"; charset=utf-8")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func protobufCreateCacheEntryRequest(metadata []byte, key, version string) []byte {
	message := appendProtobufBytesField(nil, 1, metadata)
	message = appendProtobufStringField(message, 2, key)
	message = appendProtobufStringField(message, 3, version)
	return appendProtobufStringField(message, 127, "unknown-create-field")
}

func protobufFinalizeCacheEntryUploadRequest(
	metadata []byte,
	key string,
	sizeBytes int64,
	version string,
) []byte {
	message := appendProtobufBytesField(nil, 1, metadata)
	message = appendProtobufStringField(message, 2, key)
	message = appendProtobufVarintField(message, 3, uint64(sizeBytes))
	message = appendProtobufStringField(message, 4, version)
	return appendProtobufVarintField(message, 127, 1)
}

func protobufGetCacheEntryDownloadURLRequest(
	metadata []byte,
	key string,
	restoreKeys []string,
	version string,
) []byte {
	message := appendProtobufBytesField(nil, 1, metadata)
	message = appendProtobufStringField(message, 2, key)
	for _, restoreKey := range restoreKeys {
		message = appendProtobufStringField(message, 3, restoreKey)
	}
	message = appendProtobufStringField(message, 4, version)
	return appendProtobufStringField(message, 127, "unknown-restore-field")
}

func protobufCacheMetadata(repositoryID int64, scopeName string) []byte {
	scope := appendProtobufStringField(nil, 1, scopeName)
	scope = appendProtobufVarintField(scope, 2, 3)
	metadata := appendProtobufVarintField(nil, 1, uint64(repositoryID))
	return appendProtobufBytesField(metadata, 2, scope)
}

func appendProtobufStringField(message []byte, number protowire.Number, value string) []byte {
	message = protowire.AppendTag(message, number, protowire.BytesType)
	return protowire.AppendString(message, value)
}

func appendProtobufBytesField(message []byte, number protowire.Number, value []byte) []byte {
	message = protowire.AppendTag(message, number, protowire.BytesType)
	return protowire.AppendBytes(message, value)
}

func appendProtobufVarintField(message []byte, number protowire.Number, value uint64) []byte {
	message = protowire.AppendTag(message, number, protowire.VarintType)
	return protowire.AppendVarint(message, value)
}

func decodeProtobufFields(t *testing.T, message []byte) []protobufField {
	t.Helper()
	fields := make([]protobufField, 0)
	for len(message) > 0 {
		number, typeID, tagLength := protowire.ConsumeTag(message)
		require.Greater(t, tagLength, 0, "invalid protobuf tag: %v", protowire.ParseError(tagLength))
		message = message[tagLength:]

		field := protobufField{number: number, typeID: typeID}
		switch typeID {
		case protowire.VarintType:
			value, fieldLength := protowire.ConsumeVarint(message)
			require.Greater(t, fieldLength, 0, "invalid protobuf varint: %v", protowire.ParseError(fieldLength))
			field.value = value
			message = message[fieldLength:]
		case protowire.BytesType:
			value, fieldLength := protowire.ConsumeBytes(message)
			require.Greater(t, fieldLength, 0, "invalid protobuf bytes: %v", protowire.ParseError(fieldLength))
			field.bytes = append([]byte(nil), value...)
			message = message[fieldLength:]
		default:
			require.FailNow(t, "unexpected protobuf wire type", "field %d has type %d", number, typeID)
		}
		fields = append(fields, field)
	}
	return fields
}

func protobufVarintField(fields []protobufField, number protowire.Number) (uint64, bool) {
	for _, field := range fields {
		if field.number == number && field.typeID == protowire.VarintType {
			return field.value, true
		}
	}
	return 0, false
}

func requireProtobufVarintField(t *testing.T, fields []protobufField, number protowire.Number) uint64 {
	t.Helper()
	value, ok := protobufVarintField(fields, number)
	require.True(t, ok, "protobuf varint field %d is missing", number)
	return value
}

func requireProtobufStringField(t *testing.T, fields []protobufField, number protowire.Number) string {
	t.Helper()
	for _, field := range fields {
		if field.number == number && field.typeID == protowire.BytesType {
			return string(field.bytes)
		}
	}
	require.FailNow(t, "protobuf string field is missing", "field %d", number)
	return ""
}
