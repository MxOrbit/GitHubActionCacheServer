package baseurl

import (
	"net/http"
	"net/netip"
	"strconv"
	"strings"
)

func FromRequest(r *http.Request, override string) string {
	if base := cleanBaseURL(override); base != "" {
		return base
	}

	// X-Forwarded-* is requester-controlled; fall back to r.Host on malformed values.
	host := firstHeaderValue(r.Header.Get("X-Forwarded-Host"))
	if !validAuthority(host) {
		host = r.Host
	}
	if host == "" {
		host = r.URL.Host
	}
	if host == "" {
		return ""
	}

	proto := strings.ToLower(firstHeaderValue(r.Header.Get("X-Forwarded-Proto")))
	if proto != "http" && proto != "https" {
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

// validAuthority reports whether s is a syntactically valid host[:port]
// authority, so malformed X-Forwarded-Host values cannot break generated URLs.
func validAuthority(s string) bool {
	if s == "" {
		return false
	}

	if strings.HasPrefix(s, "[") {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return false
		}
		ip, err := netip.ParseAddr(s[1:end])
		if err != nil || !ip.Is6() || ip.Zone() != "" {
			return false
		}
		if rest := s[end+1:]; rest != "" {
			if !strings.HasPrefix(rest, ":") || !validPort(rest[1:]) {
				return false
			}
		}
		return true
	}

	host := s
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		if strings.ContainsRune(s[:i], ':') || !validPort(s[i+1:]) {
			return false // unbracketed IPv6 literal or invalid port
		}
		host = s[:i]
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Is4()
	}
	return validRegName(strings.TrimSuffix(host, "."))
}

func validPort(s string) bool {
	if s == "" || len(s) > 5 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	p, _ := strconv.Atoi(s)
	return p >= 1 && p <= 65535
}

func validRegName(host string) bool {
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		for _, r := range label {
			if r != '-' && r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
				return false
			}
		}
	}
	return true
}
