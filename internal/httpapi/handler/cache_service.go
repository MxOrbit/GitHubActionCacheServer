package handler

import "github.com/gin-gonic/gin"

func CreateCacheEntry(c *gin.Context) {
	emptyHandler(c, "CreateCacheEntry")
}

func GetCacheEntryDownloadURL(c *gin.Context) {
	emptyHandler(c, "GetCacheEntryDownloadURL")
}

func FinalizeCacheEntryUpload(c *gin.Context) {
	emptyHandler(c, "FinalizeCacheEntryUpload")
}
