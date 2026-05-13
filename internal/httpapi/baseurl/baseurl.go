package baseurl

import (
	"net/http"
	"strings"
)

func FromRequest(r *http.Request, override string) string {
	if base := cleanBaseURL(override); base != "" {
		return base
	}

	host := firstHeaderValue(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = r.Host
	}
	if host == "" {
		host = r.URL.Host
	}
	if host == "" {
		return ""
	}

	proto := firstHeaderValue(r.Header.Get("X-Forwarded-Proto"))
	if proto == "" {
		proto = "http"
		if r.TLS != nil {
			proto = "https"
		}
	}

	return proto + "://" + host
}

func cleanBaseURL(value string) string {
	return strings.TrimRight(strings.TrimSpace(value), "/")
}

func firstHeaderValue(value string) string {
	if value == "" {
		return ""
	}

	first, _, _ := strings.Cut(value, ",")
	return strings.TrimSpace(first)
}
