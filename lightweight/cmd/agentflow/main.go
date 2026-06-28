// agentflow - Lightweight Temporal engine for agent-company
//
// This is a minimal entry point that starts Temporal's core state machine
// engine with SQLite in-memory persistence, independent of the full Temporal
// Server stack (no gRPC, no membership, no frontend/matching/worker services).
//
// Architecture:
//
//	agent-company (Leader/Worker)
//	    │  MCP calls
//	    ▼
//	agentflow (this process)
//	    │  Temporal state machine engine
//	    ▼
//	SQLite (in-memory or file)
//	    │  (optional) sync
//	    ▼
//	agent-hub (collaboration layer)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"

	"go.temporal.io/server/common/config"
	temporallog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║  agentflow - Temporal lightweight engine ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()

	// ── SQLite in-memory persistence ──────────────────────
	fmt.Println("[1/3] Initializing SQLite persistence (in-memory)...")

	cfg := config.SQL{
		PluginName:        "sqlite",
		DatabaseName:      "default",
		ConnectAddr:       "localhost",
		ConnectProtocol:   "tcp",
		ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
		MaxConns:          1,
		MaxIdleConns:      1,
		MaxConnLifetime:   time.Hour,
	}

	factory := sql.NewFactory(
		cfg,
		resolver.NewNoopResolver(),
		"local",
		temporallog.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
	fmt.Println("  ✓ SQLite factory created")

	execStore, err := factory.NewExecutionStore()
	if err != nil {
		log.Fatalf("  ✗ NewExecutionStore failed: %v", err)
	}
	fmt.Println("  ✓ ExecutionStore created (SQLite in-memory)")

	// ── Expose via HTTP for testing ──────────────────────
	fmt.Println("\n[2/3] Starting HTTP health endpoint...")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"store":   "sqlite-in-memory",
			"backend": execStore.GetName(),
		})
	})

	// Future: MCP endpoints will go here
	mux.HandleFunc("/api/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		// Placeholder: list namespaces from MetadataStore
		json.NewEncoder(w).Encode(map[string]interface{}{
			"namespaces": []string{},
			"note":       "agentflow v0.1 - Temporal persistence engine",
		})
	})

	server := &http.Server{
		Addr:    "127.0.0.1:9600",
		Handler: mux,
	}

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("\nShutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	fmt.Println("\n[3/3] agentflow ready")
	fmt.Println("  ┌───────────────────────────────────┐")
	fmt.Println("  │  HTTP :9600 (health + future API) │")
	fmt.Println("  │  MCP    (stdio, to be exposed)     │")
	fmt.Println("  └───────────────────────────────────┘")
	fmt.Println()
	fmt.Printf("Backend: %s\n", execStore.GetName())

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}
