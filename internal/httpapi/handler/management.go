package handler

import "github.com/gin-gonic/gin"

func ListCacheEntries(c *gin.Context) {
	emptyHandler(c, "ListCacheEntries")
}

func DeleteCacheEntries(c *gin.Context) {
	emptyHandler(c, "DeleteCacheEntries")
}

func MatchCacheEntry(c *gin.Context) {
	emptyHandler(c, "MatchCacheEntry")
}

func GetCacheEntry(c *gin.Context) {
	emptyHandler(c, "GetCacheEntry")
}

func DeleteCacheEntry(c *gin.Context) {
	emptyHandler(c, "DeleteCacheEntry")
}

func GetStorageLocation(c *gin.Context) {
	emptyHandler(c, "GetStorageLocation")
}

func DeleteStorageLocation(c *gin.Context) {
	emptyHandler(c, "DeleteStorageLocation")
}
