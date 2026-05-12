package response

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Payload interface {
	responsePayload()
}

type JSONBody[T Payload] struct {
	ok      bool
	payload T
}

func (b JSONBody[T]) MarshalJSON() ([]byte, error) {
	fields := map[string]json.RawMessage{}

	ok, err := json.Marshal(b.ok)
	if err != nil {
		return nil, err
	}
	fields["ok"] = ok

	payload, err := json.Marshal(b.payload)
	if err != nil {
		return nil, err
	}

	payloadFields := map[string]json.RawMessage{}
	if err := json.Unmarshal(payload, &payloadFields); err != nil {
		return nil, err
	}
	for key, value := range payloadFields {
		fields[key] = value
	}

	return json.Marshal(fields)
}

type JSONResponse[T Payload] interface {
	StatusCode() int
	Body() JSONBody[T]
}

type TextResponse interface {
	StatusCode() int
	Text() string
}

type EmptyResponse interface {
	StatusCode() int
	Headers() map[string]string
}

type jsonResponse[T Payload] struct {
	status  int
	ok      bool
	payload T
}

func (r jsonResponse[T]) StatusCode() int {
	return r.status
}

func (r jsonResponse[T]) Body() JSONBody[T] {
	return JSONBody[T]{
		ok:      r.ok,
		payload: r.payload,
	}
}

type textResponse struct {
	status int
	body   string
}

func (r textResponse) StatusCode() int {
	return r.status
}

func (r textResponse) Text() string {
	return r.body
}

type emptyResponse struct {
	status  int
	headers map[string]string
}

func (r emptyResponse) StatusCode() int {
	return r.status
}

func (r emptyResponse) Headers() map[string]string {
	return r.headers
}

type EmptyPayload struct{}

func (EmptyPayload) responsePayload() {}

type CreateCacheEntryPayload struct {
	SignedUploadURL string `json:"signed_upload_url"`
}

func (CreateCacheEntryPayload) responsePayload() {}

type GetCacheEntryDownloadURLPayload struct {
	SignedDownloadURL string `json:"signed_download_url"`
	MatchedKey        string `json:"matched_key"`
}

func (GetCacheEntryDownloadURLPayload) responsePayload() {}

type FinalizeCacheEntryUploadPayload struct {
	EntryID string `json:"entry_id"`
}

func (FinalizeCacheEntryUploadPayload) responsePayload() {}

type ErrorPayload struct {
	Error string `json:"error"`
}

func (ErrorPayload) responsePayload() {}

func JSON[T Payload](c *gin.Context, response JSONResponse[T]) {
	c.JSON(response.StatusCode(), response.Body())
}

func Text(c *gin.Context, response TextResponse) {
	c.String(response.StatusCode(), response.Text())
}

func Empty(c *gin.Context, response EmptyResponse) {
	for key, value := range response.Headers() {
		c.Header(key, value)
	}
	c.Status(response.StatusCode())
}

func PlainOK(body string) TextResponse {
	return textResponse{
		status: http.StatusOK,
		body:   body,
	}
}

func CacheMiss() JSONResponse[EmptyPayload] {
	return jsonResponse[EmptyPayload]{
		status:  http.StatusOK,
		ok:      false,
		payload: EmptyPayload{},
	}
}

func CreateCacheEntry(uploadURL string) JSONResponse[CreateCacheEntryPayload] {
	return jsonResponse[CreateCacheEntryPayload]{
		status: http.StatusOK,
		ok:     true,
		payload: CreateCacheEntryPayload{
			SignedUploadURL: uploadURL,
		},
	}
}

func GetCacheEntryDownloadURL(downloadURL, matchedKey string) JSONResponse[GetCacheEntryDownloadURLPayload] {
	return jsonResponse[GetCacheEntryDownloadURLPayload]{
		status: http.StatusOK,
		ok:     true,
		payload: GetCacheEntryDownloadURLPayload{
			SignedDownloadURL: downloadURL,
			MatchedKey:        matchedKey,
		},
	}
}

func FinalizeCacheEntryUpload(entryID string) JSONResponse[FinalizeCacheEntryUploadPayload] {
	return jsonResponse[FinalizeCacheEntryUploadPayload]{
		status: http.StatusOK,
		ok:     true,
		payload: FinalizeCacheEntryUploadPayload{
			EntryID: entryID,
		},
	}
}

func AzureCreated(requestID string) EmptyResponse {
	return emptyResponse{
		status: http.StatusCreated,
		headers: map[string]string{
			"x-ms-request-id": requestID,
		},
	}
}

func NotImplemented() JSONResponse[ErrorPayload] {
	return jsonResponse[ErrorPayload]{
		status: http.StatusNotImplemented,
		ok:     false,
		payload: ErrorPayload{
			Error: "not implemented",
		},
	}
}
