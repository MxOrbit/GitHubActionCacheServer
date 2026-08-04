package handler

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"syscall"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/bufferpool"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const maxBlockListRequestBodyBytes = 8 << 20

func (h *Handler) UploadPart(c *gin.Context) {
	if c.Query("comp") == "blocklist" {
		uploadID, err := strconv.ParseInt(c.Param("uploadId"), 10, 64)
		if err != nil {
			response.JSON(c, response.Error(http.StatusBadRequest, "invalid upload id"))
			return
		}
		commit, err := h.cache.PrepareBlockListCommit(c.Request.Context(), uploadID)
		if err != nil {
			h.writeCacheError(c, err)
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBlockListRequestBodyBytes)
		blockIDs, err := parseBlockList(c.Request.Body)
		if err != nil {
			var maxBytesError *http.MaxBytesError
			if errors.As(err, &maxBytesError) {
				response.JSON(c, response.Error(http.StatusRequestEntityTooLarge, "block list body too large"))
				return
			}
			if errors.Is(err, cache.ErrBlockListTooLarge) {
				h.writeCacheError(c, err)
				return
			}
			h.logger.Debug().
				Err(err).
				Str("method", c.Request.Method).
				Str("path", c.Request.URL.Path).
				Msg("block list request body rejected")
			response.JSON(c, response.Error(http.StatusBadRequest, "invalid block list"))
			return
		}
		if err := commit.Commit(c.Request.Context(), blockIDs); err != nil {
			h.writeCacheError(c, err)
			return
		}
		response.Empty(c, response.AzureCreated(uuid.NewString()))
		return
	}

	uploadID, err := strconv.ParseInt(c.Param("uploadId"), 10, 64)
	if err != nil {
		response.JSON(c, response.Error(http.StatusBadRequest, "invalid upload id"))
		return
	}

	blockID := c.Query("blockid")
	if blockID != "" {
		if !isValidBase64BlockID(blockID) {
			response.JSON(c, response.Error(http.StatusBadRequest, "invalid block id"))
			return
		}
	}

	if c.Request.Body == nil {
		response.JSON(c, response.Error(http.StatusBadRequest, "request body must be a stream"))
		return
	}
	defer c.Request.Body.Close()

	if blockID != "" {
		if err := h.cache.UploadBlock(c.Request.Context(), uploadID, blockID, c.Request.Body); err != nil {
			h.writeCacheError(c, err)
			return
		}
	} else {
		if err := h.cache.UploadPart(c.Request.Context(), uploadID, c.Request.Body); err != nil {
			h.writeCacheError(c, err)
			return
		}
	}

	response.Empty(c, response.AzureCreated(uuid.NewString()))
}

func (h *Handler) DownloadCacheEntry(c *gin.Context) {
	cacheEntryID := c.Param("cacheEntryId")
	if !h.downloadSigner.Verify(cacheEntryID, c.Query("expires"), c.Query("signature")) {
		response.JSON(c, response.Error(http.StatusUnauthorized, "invalid or expired download signature"))
		return
	}

	stream, err := h.cache.Download(c.Request.Context(), cacheEntryID)
	if err != nil {
		if errors.Is(err, cache.ErrCacheNotFound) {
			var downloadErr *cache.DownloadError
			if errors.As(err, &downloadErr) {
				h.logDownloadFailure(c.Request.Context(), err, downloadFailureMetadata(err, cacheEntryID), "open")
			}
			response.JSON(c, response.Error(http.StatusNotFound, "cache file not found"))
			return
		}
		h.logDownloadFailure(c.Request.Context(), err, downloadFailureMetadata(err, cacheEntryID), "open")
		response.JSON(c, response.Error(http.StatusInternalServerError, response.InternalServerErrorMessage))
		return
	}

	metadata := downloadMetadata{
		cacheEntryID:      stream.CacheEntryID,
		storageLocationID: stream.StorageLocationID,
		representation:    stream.Representation,
	}
	defer func() {
		if err := stream.Close(); err != nil {
			h.logDownloadFailure(c.Request.Context(), err, metadata, "close")
		}
	}()

	// Opaque cache payloads: pin Content-Type so net/http never sniffs them;
	// the rest is browser-side hardening.
	c.Header("Content-Type", "application/octet-stream")
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Content-Disposition", "attachment")
	c.Header("Cache-Control", "no-store")
	if stream.ContentLength >= 0 {
		c.Header("Content-Length", strconv.FormatInt(stream.ContentLength, 10))
	}
	written, copyErr := bufferpool.Copy(c.Writer, stream)
	if copyErr == nil && stream.ContentLength >= 0 && written != stream.ContentLength {
		copyErr = fmt.Errorf("download length mismatch: wrote %d bytes, expected %d", written, stream.ContentLength)
	}
	if copyErr != nil {
		h.logDownloadFailure(c.Request.Context(), copyErr, metadata, "stream")
		if written != 0 || c.Writer.Written() {
			return
		}
		// The payload headers no longer apply when the stream failed before the
		// response began and we can still return a structured error body.
		c.Writer.Header().Del("Content-Length")
		c.Writer.Header().Del("Content-Type")
		c.Writer.Header().Del("Content-Disposition")
		if errors.Is(copyErr, cache.ErrCacheNotFound) {
			response.JSON(c, response.Error(http.StatusNotFound, "cache file not found"))
			return
		}
		response.JSON(c, response.Error(http.StatusInternalServerError, response.InternalServerErrorMessage))
		return
	}
}

type downloadMetadata struct {
	cacheEntryID      string
	storageLocationID string
	representation    string
}

func downloadFailureMetadata(err error, fallbackCacheEntryID string) downloadMetadata {
	metadata := downloadMetadata{cacheEntryID: fallbackCacheEntryID}
	var downloadErr *cache.DownloadError
	if errors.As(err, &downloadErr) {
		metadata.cacheEntryID = downloadErr.CacheEntryID
		metadata.storageLocationID = downloadErr.StorageLocationID
		metadata.representation = downloadErr.Representation
	}
	return metadata
}

func (h *Handler) logDownloadFailure(ctx context.Context, err error, metadata downloadMetadata, stage string) {
	event := h.logger.Error()
	message := "download stream failed"
	if isExpectedDownloadCancellation(ctx, err) {
		event = h.logger.Debug()
		message = "download stream canceled"
	}
	event.
		Err(err).
		Str("cache_entry_id", metadata.cacheEntryID).
		Str("storage_location_id", metadata.storageLocationID).
		Str("representation", metadata.representation).
		Str("stage", stage).
		Msg(message)
}

func isExpectedDownloadCancellation(ctx context.Context, err error) bool {
	return errors.Is(ctx.Err(), context.Canceled) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET)
}

func isValidBase64BlockID(blockIDBase64 string) bool {
	_, err := base64.StdEncoding.DecodeString(blockIDBase64)
	return err == nil
}

func parseBlockList(stream io.Reader) ([]string, error) {
	decoder := xml.NewDecoder(stream)
	var blockIDs []string
	blockCount := 0

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return blockIDs, nil
		}
		if err != nil {
			return nil, err
		}

		start, ok := token.(xml.StartElement)
		if !ok || !isBlockListElement(start.Name.Local) {
			continue
		}
		if blockCount == cache.MaxBlockListEntries {
			return nil, cache.ErrBlockListTooLarge
		}
		blockCount++

		var blockID string
		if err := decoder.DecodeElement(&blockID, &start); err != nil {
			return nil, err
		}
		if blockID != "" {
			blockIDs = append(blockIDs, blockID)
		}
	}
}

func isBlockListElement(name string) bool {
	return name == "Latest" || name == "Uncommitted" || name == "Committed"
}

func formatInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}
