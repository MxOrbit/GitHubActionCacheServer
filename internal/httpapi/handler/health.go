package handler

import (
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

func Root(c *gin.Context) {
	response.Text(c, response.PlainOK("OK"))
}

func Health(c *gin.Context) {
	response.Text(c, response.PlainOK("healthy"))
}
