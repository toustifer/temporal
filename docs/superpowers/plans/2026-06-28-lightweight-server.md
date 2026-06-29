# Lightweight Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `pkg/server` for agentflow so agent-company can call the lightweight engine through MCP stdio and best-effort sync task state to agent-hub.

**Architecture:** `pkg/server` is the MCP boundary. It translates stdio JSON-RPC requests into `pkg/engine` calls, maps engine errors into stable MCP errors, and attempts agent-hub sync after each key operation without blocking the main result. `cmd/agentflow` will wire the server, engine, and config together.

**Tech Stack:** Go 1.26.4, Temporal `common/persistence/sql` + SQLite, MCP stdio JSON-RPC, existing `agent-hub` HTTP/MCP contract.

---

### Task 1: Define server package boundaries

**Files:**
- Create: `lightweight/pkg/server/server.go`
- Create: `lightweight/pkg/server/types.go`
- Create: `lightweight/pkg/server/errors.go`
- Test: `lightweight/pkg/server/server_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestNewServerRequiresEngine(t *testing.T) {
	_, err := New(nil, Config{})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEngineRequired)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/pkg/server -run TestNewServerRequiresEngine -v`
Expected: FAIL because `pkg/server` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```go
package server

import "go.temporal.io/server/lightweight/pkg/engine"

type Config struct {
	HubBusinessCode string
	HubEnabled      bool
}

type Server struct {
	engine *engine.Engine
	cfg    Config
}

func New(e *engine.Engine, cfg Config) (*Server, error) {
	if e == nil {
		return nil, ErrEngineRequired
	}
	return &Server{engine: e, cfg: cfg}, nil
}
```

```go
package server

import "errors"

var ErrEngineRequired = errors.New("engine required")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/pkg/server -run TestNewServerRequiresEngine -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/pkg/server
git commit -m "feat(lightweight): define server package boundary"
```

---

### Task 2: Implement MCP tool registry and request routing

**Files:**
- Create: `lightweight/pkg/server/mcp.go`
- Modify: `lightweight/pkg/server/server.go`
- Test: `lightweight/pkg/server/mcp_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestToolRegistryIncludesCoreTools(t *testing.T) {
	server, err := New(fakeEngine(), Config{})
	require.NoError(t, err)

	tools := server.Tools()
	require.Contains(t, tools, "namespace_create")
	require.Contains(t, tools, "namespace_list")
	require.Contains(t, tools, "task_create")
	require.Contains(t, tools, "task_transition")
	require.Contains(t, tools, "task_get")
	require.Contains(t, tools, "task_list")
	require.Contains(t, tools, "task_history")
	require.Contains(t, tools, "flow_ping")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/pkg/server -run TestToolRegistryIncludesCoreTools -v`
Expected: FAIL because registry/routing does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```go
func (s *Server) Tools() []ToolSpec {
	return []ToolSpec{
		{Name: "namespace_create"},
		{Name: "namespace_list"},
		{Name: "task_create"},
		{Name: "task_transition"},
		{Name: "task_get"},
		{Name: "task_list"},
		{Name: "task_history"},
		{Name: "flow_ping"},
	}
}
```

```go
type ToolSpec struct {
	Name string
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/pkg/server -run TestToolRegistryIncludesCoreTools -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/pkg/server
git commit -m "feat(lightweight): add MCP tool registry"
```

---

### Task 3: Implement engine-backed MCP handlers

**Files:**
- Create: `lightweight/pkg/server/handlers.go`
- Modify: `lightweight/pkg/server/server.go`
- Test: `lightweight/pkg/server/handlers_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestTaskCreateHandlerCallsEngine(t *testing.T) {
	eng := newRecordingEngine()
	srv, err := New(eng, Config{})
	require.NoError(t, err)

	res, err := srv.Handle(context.Background(), "task_create", map[string]any{
		"namespace_id": "ns-1",
		"task_id": "T1",
		"title": "bootstrap",
		"assigned_worker": "worker-ops",
	})
	require.NoError(t, err)
	require.Equal(t, "T1", res["id"])
	require.Equal(t, "ns-1", eng.lastCreateTask.NamespaceID)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/pkg/server -run TestTaskCreateHandlerCallsEngine -v`
Expected: FAIL because handler dispatch does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```go
func (s *Server) Handle(ctx context.Context, tool string, input map[string]any) (map[string]any, error) {
	switch tool {
	case "task_create":
		// parse input, call engine.CreateTask, return map
	case "task_transition":
		// parse input, call engine.TransitionTask, return map
	case "task_get":
		// call engine.GetTask
	case "task_list":
		// call engine.ListTasks
	case "task_history":
		// call engine.GetHistory
	case "namespace_create":
		// call engine.CreateNamespace
	case "namespace_list":
		// call engine.ListNamespaces
	case "flow_ping":
		return map[string]any{"ok": true}, nil
	default:
		return nil, ErrUnknownTool
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/pkg/server -run TestTaskCreateHandlerCallsEngine -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/pkg/server
git commit -m "feat(lightweight): route MCP tools to engine"
```

---

### Task 4: Add best-effort agent-hub sync

**Files:**
- Create: `lightweight/pkg/server/sync.go`
- Modify: `lightweight/pkg/server/server.go`
- Test: `lightweight/pkg/server/sync_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestHubSyncFailureDoesNotFailTool(t *testing.T) {
	srv, err := New(fakeEngine(), Config{HubEnabled: true})
	require.NoError(t, err)
	srv.hub = &failingHub{}

	_, err = srv.Handle(context.Background(), "flow_ping", map[string]any{})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/pkg/server -run TestHubSyncFailureDoesNotFailTool -v`
Expected: FAIL because sync hook does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```go
type HubSyncer interface {
	SyncTask(ctx context.Context, task engine.Task) error
	SyncNamespace(ctx context.Context, ns engine.Namespace) error
}

func (s *Server) syncAfterTool(ctx context.Context, tool string, payload map[string]any) {
	if s.hub == nil {
		return
	}
	_ = s.hub.SyncTask(ctx, ...)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/pkg/server -run TestHubSyncFailureDoesNotFailTool -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/pkg/server
git commit -m "feat(lightweight): add best-effort hub sync"
```

---

### Task 5: Wire server into agentflow binary

**Files:**
- Modify: `lightweight/cmd/agentflow/main.go`
- Modify: `lightweight/pkg/server/server.go`
- Test: `lightweight/cmd/agentflow/main_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestMainBuildsServer(t *testing.T) {
	_, err := BuildAgentflow(Config{DBPath: ":memory:"})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/cmd/agentflow -run TestMainBuildsServer -v`
Expected: FAIL because build wiring does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```go
func BuildAgentflow(cfg Config) (*server.Server, error) {
	eng, err := engine.NewEngine(engine.NewEngineConfig{DBPath: cfg.DBPath})
	if err != nil {
		return nil, err
	}
	return server.New(eng, server.Config{HubBusinessCode: cfg.HubBusinessCode, HubEnabled: cfg.HubEnabled})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test ./lightweight/cmd/agentflow -run TestMainBuildsServer -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/cmd/agentflow/main.go lightweight/cmd/agentflow/main_test.go lightweight/pkg/server
git commit -m "feat(lightweight): wire agentflow server into binary"
```

---

### Task 6: Verify end-to-end local startup

**Files:**
- Modify: `lightweight/cmd/agentflow/main.go`
- Test: manual run only

- [ ] **Step 1: Run the binary**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go run ./lightweight/cmd/agentflow`
Expected: prints health status and keeps HTTP/MCP server alive.

- [ ] **Step 2: Probe health**

Run: `curl http://127.0.0.1:9600/health`
Expected: JSON with `status: ok` and SQLite backend info.

- [ ] **Step 3: Exercise one MCP path**

Use the configured MCP client and call `flow_ping`.
Expected: tool returns success and does not require agent-hub connectivity.

- [ ] **Step 4: Commit**

```bash
git add lightweight/cmd/agentflow/main.go lightweight/pkg/server
git commit -m "test(lightweight): verify agentflow startup and MCP health"
```

---

## Self-check coverage

- Server boundary definition → Task 1
- MCP tool registry → Task 2
- Engine-backed handlers → Task 3
- Best-effort hub sync → Task 4
- Binary wiring → Task 5
- End-to-end startup verification → Task 6
