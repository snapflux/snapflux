package auth

import (
	"context"
	"net/http"
	"strings"
)

type contextKey struct{}

// ClientName returns the authenticated client name stored in the context by Middleware.
func ClientName(ctx context.Context) string {
	if name, ok := ctx.Value(contextKey{}).(string); ok {
		return name
	}
	return ""
}

// Middleware validates Bearer tokens against a client key map and an
// independent broker key. If both are empty, all requests are allowed.
type Middleware struct {
	tokenToName map[string]string // client key → name
	brokerKey   string
}

// New builds a Middleware. nameToKey is the client API key map; brokerKey is
// the separate credential used by peer brokers — it need not appear in nameToKey.
func New(nameToKey map[string]string, brokerKey string) *Middleware {
	m := &Middleware{
		tokenToName: make(map[string]string, len(nameToKey)),
		brokerKey:   brokerKey,
	}
	for name, key := range nameToKey {
		m.tokenToName[key] = name
	}
	return m
}

// Protect wraps h, requiring a valid Bearer token on every request.
func (m *Middleware) Protect(h http.Handler) http.Handler {
	if len(m.tokenToName) == 0 && m.brokerKey == "" {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r)
		if token == m.brokerKey && m.brokerKey != "" {
			r = r.WithContext(context.WithValue(r.Context(), contextKey{}, "broker"))
			h.ServeHTTP(w, r)
			return
		}
		name, ok := m.tokenToName[token]
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="snapflux"`)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), contextKey{}, name))
		h.ServeHTTP(w, r)
	})
}

func bearerToken(r *http.Request) string {
	after, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(after)
}
