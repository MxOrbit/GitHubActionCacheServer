package handler

import (
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

func emptyHandler(c *gin.Context, _ string) {
	response.JSON(c, response.NotImplemented())
}
