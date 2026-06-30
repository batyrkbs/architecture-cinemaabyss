package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
	mathrand "math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	Port                   string
	MonolithURL            *url.URL
	MoviesURL              *url.URL
	EventsURL              *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("invalid proxy configuration: %v", err)
	}

	seedRandom()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/api/movies", routeMovies(cfg))
	mux.HandleFunc("/api/movies/", routeMovies(cfg))
	mux.HandleFunc("/api/events", reverseProxy(cfg.EventsURL, "events-service"))
	mux.HandleFunc("/api/events/", reverseProxy(cfg.EventsURL, "events-service"))
	mux.HandleFunc("/", reverseProxy(cfg.MonolithURL, "monolith"))

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           requestLogMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("proxy-service started on port %s; movies migration=%d%% gradual=%t", cfg.Port, cfg.MoviesMigrationPercent, cfg.GradualMigration)
	log.Fatal(server.ListenAndServe())
}

func loadConfig() (config, error) {
	port := env("PORT", "8000")
	monolithURL, err := parseRequiredURL("MONOLITH_URL")
	if err != nil {
		return config{}, err
	}
	moviesURL, err := parseRequiredURL("MOVIES_SERVICE_URL")
	if err != nil {
		return config{}, err
	}
	eventsURL, err := parseRequiredURL("EVENTS_SERVICE_URL")
	if err != nil {
		return config{}, err
	}

	percent, err := strconv.Atoi(env("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil {
		return config{}, errors.New("MOVIES_MIGRATION_PERCENT must be integer")
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	return config{
		Port:                   port,
		MonolithURL:            monolithURL,
		MoviesURL:              moviesURL,
		EventsURL:              eventsURL,
		GradualMigration:       strings.EqualFold(env("GRADUAL_MIGRATION", "true"), "true"),
		MoviesMigrationPercent: percent,
	}, nil
}

func parseRequiredURL(name string) (*url.URL, error) {
	value := os.Getenv(name)
	if value == "" {
		return nil, errors.New(name + " is required")
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, errors.New(name + " must be a valid absolute URL")
	}
	return u, nil
}

func routeMovies(cfg config) http.HandlerFunc {
	monolith := reverseProxy(cfg.MonolithURL, "monolith")
	movies := reverseProxy(cfg.MoviesURL, "movies-service")
	return func(w http.ResponseWriter, r *http.Request) {
		if shouldRouteToMovies(cfg) {
			movies(w, r)
			return
		}
		monolith(w, r)
	}
}

func shouldRouteToMovies(cfg config) bool {
	if !cfg.GradualMigration {
		return false
	}
	switch cfg.MoviesMigrationPercent {
	case 0:
		return false
	case 100:
		return true
	default:
		return mathrand.Intn(100) < cfg.MoviesMigrationPercent
	}
}

func reverseProxy(target *url.URL, serviceName string) http.HandlerFunc {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-CinemaAbyss-Target", serviceName)
	}
	proxy.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Set("X-CinemaAbyss-Target", serviceName)
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error target=%s method=%s path=%s err=%v", serviceName, r.Method, r.URL.Path, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   "upstream_unavailable",
			"service": serviceName,
		})
	}
	return func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w, r)
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func requestLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s duration=%s", r.Method, r.URL.RequestURI(), time.Since(start).String())
	})
}

func seedRandom() {
	var b [8]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err == nil {
		mathrand.Seed(int64(binary.LittleEndian.Uint64(b[:])))
		return
	}
	mathrand.Seed(time.Now().UnixNano())
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
