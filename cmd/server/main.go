package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/prashkn/sales-tax-api/internal/apikey"
	"github.com/prashkn/sales-tax-api/internal/cache"
	"github.com/prashkn/sales-tax-api/internal/config"
	"github.com/prashkn/sales-tax-api/internal/handler"
	"github.com/prashkn/sales-tax-api/internal/service"
	"github.com/prashkn/sales-tax-api/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Database
	db, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Cache
	rdb, err := cache.New(cfg.RedisURL, cfg.CacheTTLHrs)
	if err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	// Services
	taxService := service.NewTaxService(db, rdb)

	// Handlers
	taxHandler := handler.NewTaxHandler(taxService)
	healthHandler := handler.NewHealthHandler(db, rdb)
	keyValidator := apikey.NewValidator(cfg.APIKeySecret)

	// Router
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RealIP)
	r.Use(handler.RequestLogger)

	// Public
	r.Get("/v1/health", healthHandler.Health)

	// Authenticated
	r.Group(func(r chi.Router) {
		r.Use(handler.APIKeyAuth(keyValidator))

		r.Get("/v1/tax/zip/{zip_code}", taxHandler.LookupByZIP)
		r.Get("/v1/tax/address", taxHandler.LookupByAddress)
		r.Post("/v1/tax/calculate", taxHandler.Calculate)
		r.Post("/v1/tax/bulk", taxHandler.Bulk)
	})

	// Server
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		slog.Info("shutting down server")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("server shutdown error", "error", err)
		}
		cancel()
	}()

	slog.Info("starting server", "port", cfg.Port, "env", cfg.Environment)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}