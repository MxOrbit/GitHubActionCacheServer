package handler

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/bufferpool"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func (h *Handler) UploadPart(c *gin.Context) {
	if c.Query("comp") == "blocklist" {
		uploadID, err := strconv.ParseInt(c.Param("uploadId"), 10, 64)
		if err != nil {
			response.JSON(c, response.Error(http.StatusBadRequest, "invalid upload id"))
			return
		}
		blockIDs, err := parseBlockList(c.Request.Body)
		if err != nil {
			response.JSON(c, response.Error(http.StatusBadRequest, "invalid block list"))
			return
		}
		if err := h.cache.CommitBlockList(c.Request.Context(), uploadID, blockIDs); err != nil {
			writeCacheError(c, err)
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
			writeCacheError(c, err)
			return
		}
	} else {
		if err := h.cache.UploadPart(c.Request.Context(), uploadID, c.Request.Body); err != nil {
			writeCacheError(c, err)
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
			response.JSON(c, response.Error(http.StatusNotFound, "cache file not found"))
			return
		}
		writeCacheError(c, err)
		return
	}
	defer stream.Close()

	written, err := bufferpool.Copy(c.Writer, stream)
	if err != nil && written == 0 && !c.Writer.Written() {
		if errors.Is(err, cache.ErrCacheNotFound) {
			response.JSON(c, response.Error(http.StatusNotFound, "cache file not found"))
			return
		}
		writeCacheError(c, err)
	}
}

func isValidBase64BlockID(blockIDBase64 string) bool {
	_, err := base64.StdEncoding.DecodeString(blockIDBase64)
	return err == nil
}

func parseBlockList(stream io.Reader) ([]string, error) {
	decoder := xml.NewDecoder(stream)
	var blockIDs []string

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
