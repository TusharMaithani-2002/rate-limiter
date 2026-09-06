package middleware

import (
	"net"
	"net/http"
	"strings"
)

// ExtractClientIP retrieves the real client IP address from request headers or TCP connection.
func ExtractClientIP(r *http.Request) string {

	// Check Cloudflare's direct client header
	if cfIP := r.Header.Get("CF-Connecting-IP"); cfIP != "" {
		return strings.TrimSpace(cfIP)
	}

	// Check X-Forwarded-For (comma-separated if through multiple proxies)
	// Example: "203.0.113.195, 70.41.3.18, 150.172.238.178"
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		clientIP := strings.TrimSpace(parts[0])
		if clientIP != "" {
			return clientIP
		}
	}

	// 3. Fallback to direct TCP connection remote address
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}
