package handler

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/middleware"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	twirppb "github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/twirp"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"google.golang.org/protobuf/proto"
)

const (
	protobufContentType             = "application/protobuf"
	maxCacheServiceRequestBodyBytes = 128 << 10
)

type cacheWireFormat uint8

const (
	cacheWireJSON cacheWireFormat = iota
	cacheWireProtobuf
)

type cacheRequestDecoder[T keyedCacheRequest] func([]byte) (T, error)

func bindCacheRequest[T keyedCacheRequest](
	c *gin.Context,
	decodeProtobuf cacheRequestDecoder[T],
) (T, auth.CacheScope, cacheWireFormat, bool) {
	var body T
	wireFormat := cacheRequestWireFormat(c.GetHeader("Content-Type"))
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxCacheServiceRequestBodyBytes)
	raw, err := io.ReadAll(c.Request.Body)
	if err == nil {
		if wireFormat == cacheWireProtobuf {
			body, err = decodeProtobuf(raw)
		} else {
			err = binding.JSON.BindBody(raw, &body)
		}
	}
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			response.JSON(c, response.Error(http.StatusRequestEntityTooLarge, "request body too large"))
			return body, auth.CacheScope{}, wireFormat, false
		}
		response.JSON(c, response.Error(http.StatusBadRequest, "invalid body"))
		return body, auth.CacheScope{}, wireFormat, false
	}
	if body.cacheKey() == "" || body.cacheVersion() == "" {
		response.JSON(c, response.Error(http.StatusBadRequest, "invalid body"))
		return body, auth.CacheScope{}, wireFormat, false
	}

	scope, ok := middleware.CacheScope(c)
	if !ok {
		response.JSON(c, response.Error(http.StatusUnauthorized, "cache scope missing"))
		return body, auth.CacheScope{}, wireFormat, false
	}

	return body, scope, wireFormat, true
}

func cacheRequestWireFormat(contentType string) cacheWireFormat {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil && strings.EqualFold(mediaType, protobufContentType) {
		return cacheWireProtobuf
	}
	return cacheWireJSON
}

func writeCacheResponse[T response.Payload](
	c *gin.Context,
	wireFormat cacheWireFormat,
	jsonResponse response.JSONResponse[T],
	protobufResponse proto.Message,
) {
	if wireFormat == cacheWireJSON {
		response.JSON(c, jsonResponse)
		return
	}

	body, err := proto.Marshal(protobufResponse)
	if err != nil {
		response.JSON(c, response.Error(http.StatusInternalServerError, "failed to encode response"))
		return
	}
	c.Data(jsonResponse.StatusCode(), protobufContentType, body)
}

func decodeCreateCacheEntryRequest(raw []byte) (createCacheEntryRequest, error) {
	var message twirppb.CreateCacheEntryRequest
	if err := proto.Unmarshal(raw, &message); err != nil {
		return createCacheEntryRequest{}, err
	}
	return createCacheEntryRequest{
		Key:     message.GetKey(),
		Version: message.GetVersion(),
	}, nil
}

func decodeGetCacheEntryDownloadURLRequest(raw []byte) (getCacheEntryDownloadURLRequest, error) {
	var message twirppb.GetCacheEntryDownloadURLRequest
	if err := proto.Unmarshal(raw, &message); err != nil {
		return getCacheEntryDownloadURLRequest{}, err
	}
	return getCacheEntryDownloadURLRequest{
		Key:         message.GetKey(),
		RestoreKeys: append([]string(nil), message.GetRestoreKeys()...),
		Version:     message.GetVersion(),
	}, nil
}

func decodeFinalizeCacheEntryUploadRequest(raw []byte) (finalizeCacheEntryUploadRequest, error) {
	var message twirppb.FinalizeCacheEntryUploadRequest
	if err := proto.Unmarshal(raw, &message); err != nil {
		return finalizeCacheEntryUploadRequest{}, err
	}
	return finalizeCacheEntryUploadRequest{
		Key:       message.GetKey(),
		SizeBytes: message.GetSizeBytes(),
		Version:   message.GetVersion(),
	}, nil
}
