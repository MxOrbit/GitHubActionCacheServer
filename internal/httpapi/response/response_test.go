package response

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestJSONResponseBodyMarshal(t *testing.T) {
	tests := []struct {
		name string
		body any
		want string
	}{
		{
			name: "cache miss",
			body: CacheMiss().Body(),
			want: `{"ok":false}`,
		},
		{
			name: "create cache entry",
			body: CreateCacheEntry("http://example.com/upload").Body(),
			want: `{"ok":true,"signed_upload_url":"http://example.com/upload"}`,
		},
		{
			name: "get cache entry download url",
			body: GetCacheEntryDownloadURL("http://example.com/download", "cache-key").Body(),
			want: `{"ok":true,"signed_download_url":"http://example.com/download","matched_key":"cache-key"}`,
		},
		{
			name: "finalize cache entry upload",
			body: FinalizeCacheEntryUpload("123").Body(),
			want: `{"ok":true,"entry_id":"123"}`,
		},
		{
			name: "not implemented",
			body: NotImplemented().Body(),
			want: `{"ok":false,"error":"not implemented"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.body)

			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestInternalErrorHidesAndRecordsCause(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	internalErr := errors.New("sensitive database detail")

	InternalError(c, internalErr)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.JSONEq(t, `{"ok":false,"error":"internal server error"}`, recorder.Body.String())
	require.NotContains(t, recorder.Body.String(), internalErr.Error())
	require.Len(t, c.Errors.ByType(gin.ErrorTypePrivate), 1)
	require.ErrorIs(t, c.Errors[0].Err, internalErr)
}
