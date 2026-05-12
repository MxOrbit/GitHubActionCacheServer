package config

import (
	"os"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/tools"
)

const (
	DefaultAddr         = ":3000"
	DefaultTokenIssuer  = "https://token.actions.githubusercontent.com"
	DefaultTokenJWKSURL = "https://token.actions.githubusercontent.com/.well-known/jwks"
)

type Config struct {
	Server ServerConfig
	Auth   AuthConfig
}

type ServerConfig struct {
	Addr string
}

type AuthConfig struct {
	TokenIssuer         string
	TokenJWKSURL        string
	SkipTokenValidation bool
}

func Load() Config {
	return Config{
		Server: ServerConfig{
			Addr: envOrDefault("ADDR", DefaultAddr),
		},
		Auth: AuthConfig{
			TokenIssuer:         envOrDefault("GITHUB_ACTIONS_TOKEN_ISSUER", DefaultTokenIssuer),
			TokenJWKSURL:        envOrDefault("GITHUB_ACTIONS_TOKEN_JWKS_URL", DefaultTokenJWKSURL),
			SkipTokenValidation: tools.ParseBool(envOrDefault("SKIP_TOKEN_VALIDATION", "false")),
		},
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
