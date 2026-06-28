package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"

	"go.temporal.io/server/common/config"
	temporallog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/lightweight/pkg/engine"
	lwserver "go.temporal.io/server/lightweight/pkg/server"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "stdio" {
		if err := runMCPStdio(); err != nil {
			log.Fatalf("mcp server exited: %v", err)
		}
		return
	}

	if err := runHTTP(); err != nil {
		log.Fatalf("agentflow startup failed: %v", err)
	}
}

func runHTTP() error {
	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║  agentflow - Temporal lightweight engine ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()

	components, err := buildComponents()
	if err != nil {
		return err
	}
	defer components.close()

	fmt.Println("[1/3] Lightweight engine and server assembled")
	fmt.Printf("  ✓ Engine ready: %T\n", components.engine)
	fmt.Printf("  ✓ Server ready: %T\n", components.server)
	fmt.Printf("  ✓ Server tools (%d): %s\n", len(components.tools), strings.Join(toolNames(components.tools), ", "))
	fmt.Printf("  ✓ Persistence probe: %s\n", components.backendName)

	fmt.Println("\n[2/3] Starting HTTP health endpoint...")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"store":   "sqlite-in-memory",
			"backend": components.backendName,
		})
	})

	mux.HandleFunc("/api/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"namespaces": []string{},
			"note":       "agentflow v0.1 - Temporal persistence engine",
		})
	})

	httpServer := &http.Server{Addr: "127.0.0.1:9600", Handler: mux}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("\nShutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}()

	fmt.Println("\n[3/3] agentflow ready")
	fmt.Println("  ┌───────────────────────────────────┐")
	fmt.Println("  │  HTTP :9600 (health + future API) │")
	fmt.Println("  │  MCP    (assembled, not served)   │")
	fmt.Println("  └───────────────────────────────────┘")
	fmt.Println()
	fmt.Printf("Backend: %s\n", components.backendName)

	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func runMCPStdio() error {
	components, err := buildComponents()
	if err != nil {
		return err
	}
	defer components.close()

	return serveMCP(context.Background(), os.Stdin, os.Stdout, components.server)
}

type runtimeComponents struct {
	engine      *engine.Engine
	server      *lwserver.Server
	tools       []lwserver.ToolSpec
	backendName string
	cleanup     func() error
}

func buildComponents() (*runtimeComponents, error) {
	backendName, cleanup, err := probeSQLiteBackend()
	if err != nil {
		return nil, err
	}

	eng, err := engine.NewEngine(engine.NewEngineConfig{})
	if err != nil {
		if cleanup != nil {
			_ = cleanup()
		}
		return nil, err
	}

	srv, err := lwserver.New(eng, lwserver.Config{})
	if err != nil {
		_ = eng.Close()
		if cleanup != nil {
			_ = cleanup()
		}
		return nil, err
	}

	return &runtimeComponents{
		engine:      eng,
		server:      srv,
		tools:       srv.Tools(),
		backendName: backendName,
		cleanup: func() error {
			engErr := eng.Close()
			if cleanup == nil {
				return engErr
			}
			cleanupErr := cleanup()
			if engErr != nil {
				return engErr
			}
			return cleanupErr
		},
	}, nil
}

func probeSQLiteBackend() (string, func() error, error) {
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

	execStore, err := factory.NewExecutionStore()
	if err != nil {
		return "", nil, fmt.Errorf("NewExecutionStore failed: %w", err)
	}

	backendName := execStore.GetName()
	return backendName, func() error {
		execStore.Close()
		return nil
	}, nil
}

func toolNames(tools []lwserver.ToolSpec) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}

func (c *runtimeComponents) close() {
	if c == nil || c.cleanup == nil {
		return
	}
	if err := c.cleanup(); err != nil {
		log.Printf("component shutdown error: %v", err)
	}
}

type rpcRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	Method   string         `json:"method"`
	Params   map[string]any `json:"params"`
	ID       any            `json:"id"`
}

type rpcResponse struct {
	JSONRPC string         `json:"jsonrpc"`
	Result  any            `json:"result,omitempty"`
	Error   *rpcError      `json:"error,omitempty"`
	ID      any            `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func serveMCP(ctx context.Context, in io.Reader, out io.Writer, srv *lwserver.Server) error {
	reader := bufio.NewReader(in)
	writer := bufio.NewWriter(out)
	defer writer.Flush()

	for {
		req, err := decodeRPC(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case "initialize":
			resp.Result = map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities": map[string]any{
					"tools": map[string]any{},
				},
				"serverInfo": map[string]any{
					"name":    "agentflow",
					"version": "0.1.0",
				},
			}
		case "tools/list":
			resp.Result = map[string]any{"tools": srv.Tools()}
		case "tools/call":
			name, _ := req.Params["name"].(string)
			args, _ := req.Params["arguments"].(map[string]any)
			data, callErr := srv.Handle(ctx, name, args)
			if callErr != nil {
				resp.Error = &rpcError{Code: -32603, Message: callErr.Error()}
			} else {
				resp.Result = map[string]any{"content": []any{map[string]any{"type": "text", "text": formatToolResult(data)}}}
			}
		default:
			resp.Error = &rpcError{Code: -32601, Message: fmt.Sprintf("method not found: %s", req.Method)}
		}

		if err := encodeRPC(writer, resp); err != nil {
			return err
		}
	}
}

func decodeRPC(r *bufio.Reader) (*rpcRequest, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(line))) == 0 {
		return decodeRPC(r)
	}

	var req rpcRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func encodeRPC(w *bufio.Writer, resp rpcResponse) error {
	payload, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	if _, err := w.Write(append(payload, '\n')); err != nil {
		return err
	}
	return w.Flush()
}

func formatToolResult(v any) string {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(payload)
}
