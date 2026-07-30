package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestWriteCacheErrorHidesWrappedDetails(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/cache", nil)
	handler := &Handler{logger: zerolog.Nop()}

	handler.writeCacheError(c, fmt.Errorf("%w: sensitive storage detail", cache.ErrPartCountMismatch))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.JSONEq(
		t,
		`{"ok":false,"error":"uploaded part count does not match actual part count in storage"}`,
		recorder.Body.String(),
	)
	require.NotContains(t, recorder.Body.String(), "sensitive storage detail")
}
