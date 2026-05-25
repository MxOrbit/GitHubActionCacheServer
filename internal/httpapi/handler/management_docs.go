package handler

import (
	"fmt"
	"net/http"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/baseurl"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

const managementDocsHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cache Server Management API</title>
  <style>
    body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #15171a; background: #f6f7f9; }
    header { padding: 24px 32px; background: #ffffff; border-bottom: 1px solid #d7dce2; }
    h1 { margin: 0 0 8px; font-size: 24px; line-height: 1.2; }
    p { margin: 0; color: #56616f; }
    main { max-width: 960px; margin: 0 auto; padding: 28px 24px 40px; }
    section { background: #ffffff; border: 1px solid #d7dce2; border-radius: 8px; padding: 20px; }
    a { color: #0b63ce; }
    code { background: #eef1f5; border-radius: 4px; padding: 2px 5px; }
    ul { line-height: 1.8; }
  </style>
</head>
<body>
  <header>
    <h1>Cache Server Management API</h1>
    <p>OpenAPI reference for the cache entries and storage locations management endpoints.</p>
  </header>
  <main>
    <section>
      <p>OpenAPI spec: <a href="%s">%s</a></p>
      <p>Authenticate protected endpoints with <code>X-Api-Key</code>.</p>
      <ul>
        <li><code>GET /management-api/cache-entries/</code></li>
        <li><code>GET /management-api/cache-entries/match</code></li>
        <li><code>GET /management-api/cache-entries/{id}</code></li>
        <li><code>DELETE /management-api/cache-entries/{id}</code></li>
        <li><code>GET /management-api/storage-locations/{id}</code></li>
        <li><code>DELETE /management-api/storage-locations/{id}</code></li>
      </ul>
    </section>
  </main>
</body>
</html>
`

func (h *Handler) ManagementDocs(c *gin.Context) {
	if !h.managementAPIEnabled(c) {
		return
	}

	specURL := "./_docs/spec.json"
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(fmt.Sprintf(managementDocsHTML, specURL, specURL)))
}

func (h *Handler) ManagementOpenAPISpec(c *gin.Context) {
	if !h.managementAPIEnabled(c) {
		return
	}

	c.JSON(http.StatusOK, h.managementOpenAPISpec(c))
}

func (h *Handler) managementAPIEnabled(c *gin.Context) bool {
	if h.cfg.Management.APIKey == "" {
		response.JSON(c, response.Error(http.StatusServiceUnavailable, "management api is disabled"))
		return false
	}
	return true
}

func (h *Handler) managementOpenAPISpec(c *gin.Context) map[string]any {
	serverURL := baseurl.FromRequest(c.Request, h.cfg.Server.APIBaseURL)
	if serverURL == "" {
		serverURL = "/management-api"
	} else {
		serverURL += "/management-api"
	}

	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":   "Cache Server Management API",
			"version": "1.0.0",
		},
		"servers": []map[string]string{{"url": serverURL}},
		"security": []map[string][]string{
			{"apiKeyHeader": []string{}},
		},
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"apiKeyHeader": map[string]string{
					"type": "apiKey",
					"in":   "header",
					"name": "X-Api-Key",
				},
			},
			"schemas": managementOpenAPISchemas(),
		},
		"paths": managementOpenAPIPaths(),
	}
}

func managementOpenAPISchemas() map[string]any {
	return map[string]any{
		"CacheEntry": map[string]any{
			"type":     "object",
			"required": []string{"id", "key", "version", "scope", "repoId", "updatedAt", "locationId"},
			"properties": map[string]any{
				"id":         map[string]string{"type": "string"},
				"key":        map[string]string{"type": "string"},
				"version":    map[string]string{"type": "string"},
				"scope":      map[string]string{"type": "string"},
				"repoId":     map[string]string{"type": "string"},
				"updatedAt":  map[string]string{"type": "integer", "format": "int64"},
				"locationId": map[string]string{"type": "string"},
			},
		},
		"StorageLocation": map[string]any{
			"type":     "object",
			"required": []string{"id", "folderName", "partCount"},
			"properties": map[string]any{
				"id":               map[string]string{"type": "string"},
				"folderName":       map[string]string{"type": "string"},
				"partCount":        map[string]string{"type": "integer"},
				"mergeStartedAt":   nullableInt64Schema(),
				"mergedAt":         nullableInt64Schema(),
				"partsDeletedAt":   nullableInt64Schema(),
				"lastDownloadedAt": nullableInt64Schema(),
			},
		},
		"CacheEntryList": map[string]any{
			"type":     "object",
			"required": []string{"total", "items"},
			"properties": map[string]any{
				"total": map[string]string{"type": "integer"},
				"items": map[string]any{
					"type":  "array",
					"items": refSchema("#/components/schemas/CacheEntry"),
				},
			},
		},
		"CacheEntryMatch": map[string]any{
			"type":     "object",
			"nullable": true,
			"required": []string{"match", "type"},
			"properties": map[string]any{
				"match": refSchema("#/components/schemas/CacheEntry"),
				"type": map[string]any{
					"type": "string",
					"enum": []string{"exact-primary", "prefixed-primary", "exact-restore", "prefixed-restore"},
				},
			},
		},
		"ErrorResponse": map[string]any{
			"type":     "object",
			"required": []string{"ok", "error"},
			"properties": map[string]any{
				"ok":    map[string]string{"type": "boolean"},
				"error": map[string]string{"type": "string"},
			},
		},
	}
}

func managementOpenAPIPaths() map[string]any {
	return map[string]any{
		"/cache-entries/": map[string]any{
			"get": map[string]any{
				"tags":        []string{"Cache Entries"},
				"summary":     "List cache entries",
				"parameters":  append(cacheEntryFilterParameters(), pagingParameters()...),
				"responses":   responseSchemas(refSchema("#/components/schemas/CacheEntryList")),
				"description": "Returns cache entries ordered by most recently updated.",
			},
			"delete": map[string]any{
				"tags":        []string{"Cache Entries"},
				"summary":     "Delete cache entries",
				"parameters":  cacheEntryFilterParameters(),
				"responses":   noContentResponse(),
				"description": "Deletes matching cache entries and cleans orphaned storage locations.",
			},
		},
		"/cache-entries/match": map[string]any{
			"get": map[string]any{
				"tags":       []string{"Cache Entries"},
				"summary":    "Match a cache entry",
				"parameters": matchParameters(),
				"responses":  responseSchemas(refSchema("#/components/schemas/CacheEntryMatch")),
			},
		},
		"/cache-entries/{id}": map[string]any{
			"get": map[string]any{
				"tags":       []string{"Cache Entries"},
				"summary":    "Get a cache entry",
				"parameters": []map[string]any{pathStringParameter("id")},
				"responses":  responseSchemas(refSchema("#/components/schemas/CacheEntry")),
			},
			"delete": map[string]any{
				"tags":       []string{"Cache Entries"},
				"summary":    "Delete a cache entry",
				"parameters": []map[string]any{pathStringParameter("id")},
				"responses":  noContentResponse(),
			},
		},
		"/storage-locations/{id}": map[string]any{
			"get": map[string]any{
				"tags":       []string{"Storage Locations"},
				"summary":    "Get a storage location",
				"parameters": []map[string]any{pathStringParameter("id")},
				"responses":  responseSchemas(refSchema("#/components/schemas/StorageLocation")),
			},
			"delete": map[string]any{
				"tags":       []string{"Storage Locations"},
				"summary":    "Delete a storage location",
				"parameters": []map[string]any{pathStringParameter("id")},
				"responses":  noContentResponse(),
			},
		},
	}
}

func cacheEntryFilterParameters() []map[string]any {
	return []map[string]any{
		queryStringParameter("key", false),
		queryStringParameter("version", false),
		queryStringParameter("scope", false),
		queryStringParameter("repoId", false),
	}
}

func pagingParameters() []map[string]any {
	return []map[string]any{
		queryIntegerParameter("page", false, 1, 0),
		queryIntegerParameter("itemsPerPage", false, defaultManagementItemsPerPage, maxManagementItemsPerPage),
	}
}

func matchParameters() []map[string]any {
	return []map[string]any{
		queryStringParameter("primaryKey", true),
		queryStringParameter("version", true),
		queryStringParameter("repoId", true),
		queryArrayParameter("scopes", true),
		queryArrayParameter("restoreKeys", false),
	}
}

func queryStringParameter(name string, required bool) map[string]any {
	return map[string]any{
		"name":     name,
		"in":       "query",
		"required": required,
		"schema":   map[string]string{"type": "string"},
	}
}

func queryIntegerParameter(name string, required bool, defaultValue int, maximum int) map[string]any {
	schema := map[string]any{
		"type":    "integer",
		"default": defaultValue,
		"minimum": 1,
	}
	if maximum > 0 {
		schema["maximum"] = maximum
	}
	return map[string]any{
		"name":     name,
		"in":       "query",
		"required": required,
		"schema":   schema,
	}
}

func queryArrayParameter(name string, required bool) map[string]any {
	return map[string]any{
		"name":     name,
		"in":       "query",
		"required": required,
		"style":    "form",
		"explode":  true,
		"schema": map[string]any{
			"type":  "array",
			"items": map[string]string{"type": "string"},
		},
	}
}

func pathStringParameter(name string) map[string]any {
	return map[string]any{
		"name":     name,
		"in":       "path",
		"required": true,
		"schema":   map[string]string{"type": "string"},
	}
}

func responseSchemas(schema map[string]any) map[string]any {
	return map[string]any{
		"200": map[string]any{
			"description": "OK",
			"content": map[string]any{
				"application/json": map[string]any{
					"schema": schema,
				},
			},
		},
		"400": errorResponseSchema(),
		"401": errorResponseSchema(),
		"404": errorResponseSchema(),
		"500": errorResponseSchema(),
	}
}

func noContentResponse() map[string]any {
	return map[string]any{
		"204": map[string]string{"description": "No Content"},
		"401": errorResponseSchema(),
		"500": errorResponseSchema(),
	}
}

func errorResponseSchema() map[string]any {
	return map[string]any{
		"description": "Error",
		"content": map[string]any{
			"application/json": map[string]any{
				"schema": refSchema("#/components/schemas/ErrorResponse"),
			},
		},
	}
}

func refSchema(ref string) map[string]any {
	return map[string]any{"$ref": ref}
}

func nullableInt64Schema() map[string]any {
	return map[string]any{
		"type":     "integer",
		"format":   "int64",
		"nullable": true,
	}
}
