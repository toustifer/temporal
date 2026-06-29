# B/C Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden `agentflow` (B) into a real SQLite-backed local state service and make agent-hub sync (C) best-effort but real, while keeping the system runnable without agent-hub.

**Architecture:** `pkg/engine` becomes the source of truth for namespaces, tasks, and event history backed by Temporal SQLite persistence instead of in-memory maps. `pkg/server` keeps MCP as a thin adapter over engine and performs best-effort hub synchronization after key operations. `cmd/agentflow` stays a dual-mode binary: HTTP health by default, MCP stdio when launched with `stdio`.

**Tech Stack:** Go 1.26.4, Temporal `common/persistence/sql` + SQLite, Temporal `service/history/hsm`, MCP stdio framing, best-effort agent-hub HTTP/MCP calls.

---

### Task 1: Split engine into a persisted core and a small in-memory fallback test harness

**Files:**
- Modify: `lightweight/pkg/engine/engine.go`
- Create: `lightweight/pkg/engine/store.go`
- Create: `lightweight/pkg/engine/store_test.go`
- Modify: `lightweight/pkg/engine/engine_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestEnginePersistsNamespaceAndTaskAcrossReload(t *testing.T) {
	eng1, err := NewEngine(NewEngineConfig{DBPath: ":memory:"})
	require.NoError(t, err)
	_, err = eng1.CreateNamespace(context.Background(), CreateNamespaceRequest{ID: "ns-1", Name: "agent-company"})
	require.NoError(t, err)
	_, err = eng1.CreateTask(context.Background(), CreateTaskRequest{NamespaceID: "ns-1", ID: "T1", Title: "bootstrap", AssignedWorker: "worker-ops"})
	require.NoError(t, err)
	require.NoError(t, eng1.Close())

	eng2, err := NewEngine(NewEngineConfig{DBPath: ":memory:"})
	require.NoError(t, err)
	defer func() { require.NoError(t, eng2.Close()) }()

	_, err = eng2.GetNamespace(context.Background(), "ns-1")
	require.NoError(t, err)
	_, err = eng2.GetTask(context.Background(), "ns-1", "T1")
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test -tags test_dep ./lightweight/pkg/engine -run TestEnginePersistsNamespaceAndTaskAcrossReload -v`
Expected: FAIL because engine is still in-memory only.

- [ ] **Step 3: Write minimal implementation**

```go
type NewEngineConfig struct {
	DBPath string
}

func NewEngine(cfg NewEngineConfig) (*Engine, error) {
	store, err := openStore(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	return &Engine{store: store}, nil
}
```

```go
// store.go
package engine

type store struct {
	// wrap sqlite-backed persistence tables / caches
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test -tags test_dep ./lightweight/pkg/engine -run TestEnginePersistsNamespaceAndTaskAcrossReload -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/pkg/engine
git commit -m "feat(lightweight): persist agentflow engine state"
```

---

### Task 2: Implement SQLite-backed task history and state transitions

**Files:**
- Modify: `lightweight/pkg/engine/engine.go`
- Create: `lightweight/pkg/engine/history_test.go`
- Modify: `lightweight/pkg/engine/engine_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestEngineStoresTransitionHistory(t *testing.T) {
	eng, err := NewEngine(NewEngineConfig{DBPath: ":memory:"})
	require.NoError(t, err)
	defer func() { require.NoError(t, eng.Close()) }()

	_, err = eng.CreateNamespace(context.Background(), CreateNamespaceRequest{ID: "ns-1", Name: "agent-company"})
	require.NoError(t, err)
	_, err = eng.CreateTask(context.Background(), CreateTaskRequest{NamespaceID: "ns-1", ID: "T1", Title: "bootstrap"})
	require.NoError(t, err)
	_, err = eng.TransitionTask(context.Background(), "ns-1", "T1", TransStart, map[string]string{"actor": "leader"})
	require.NoError(t, err)

	history, err := eng.GetHistory(context.Background(), "ns-1", "T1")
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, "start", history[1].Transition)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test -tags test_dep ./lightweight/pkg/engine -run TestEngineStoresTransitionHistory -v`
Expected: FAIL because history is still memory-only.

- [ ] **Step 3: Write minimal implementation**

```go
func (e *Engine) TransitionTask(...) (*Task, error) {
	// update persisted task row
	// append history row
	// return cloned task
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test -tags test_dep ./lightweight/pkg/engine -run TestEngineStoresTransitionHistory -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/pkg/engine
git commit -m "feat(lightweight): persist agentflow history"
```

---

### Task 3: Stabilize the MCP stdio contract

**Files:**
- Modify: `lightweight/cmd/agentflow/main.go`
- Modify: `lightweight/cmd/agentflow/main_test.go`
- Create: `lightweight/cmd/agentflow/stdio_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestServeMCPInitializeAndToolsList(t *testing.T) {
	in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
`)
	var out bytes.Buffer
	components, err := buildComponents()
	require.NoError(t, err)
	defer components.close()

	require.NoError(t, serveMCP(context.Background(), in, &out, components.server))
	require.Contains(t, out.String(), `"protocolVersion":"2024-11-05"`)
	require.Contains(t, out.String(), `"name":"task_create"`)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test -tags test_dep ./lightweight/cmd/agentflow -run TestServeMCPInitializeAndToolsList -v`
Expected: FAIL if framing is not robust enough.

- [ ] **Step 3: Write minimal implementation**

```go
func serveMCP(ctx context.Context, in io.Reader, out io.Writer, srv *lwserver.Server) error {
	// handle initialize / tools/list / tools/call line-by-line
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test -tags test_dep ./lightweight/cmd/agentflow -run TestServeMCPInitializeAndToolsList -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/cmd/agentflow
git commit -m "feat(lightweight): stabilize mcp stdio contract"
```

---

### Task 4: Replace hub sync stubs with a real best-effort adapter

**Files:**
- Modify: `lightweight/pkg/server/sync.go`
- Modify: `lightweight/pkg/server/server.go`
- Modify: `lightweight/pkg/server/mcp_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestHubSyncFailureDoesNotFailTaskTransition(t *testing.T) {
	srv := newTestServerWithConfig(t, Config{HubEnabled: true})
	srv.hub = failingHubSyncer{err: errors.New("hub unavailable")}

	_, err := srv.Handle(context.Background(), "task_transition", map[string]any{
		"namespace_id": "ns-1",
		"task_id": "T-sync",
		"transition": "start",
	})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test -tags test_dep ./lightweight/pkg/server -run TestHubSyncFailureDoesNotFailTaskTransition -v`
Expected: FAIL if sync is not wired for task_transition.

- [ ] **Step 3: Write minimal implementation**

```go
func (s *Server) syncTask(ctx context.Context, task *engine.Task) {
	if s.hub == nil || task == nil { return }
	_ = s.hub.SyncTask(ctx, task)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go test -tags test_dep ./lightweight/pkg/server -run TestHubSyncFailureDoesNotFailTaskTransition -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lightweight/pkg/server
git commit -m "feat(lightweight): make hub sync best-effort"
```

---

### Task 5: End-to-end verify local runtime and sync hooks

**Files:**
- Modify: `lightweight/cmd/agentflow/main.go`
- Test: manual run only

- [ ] **Step 1: Run the binary in stdio mode**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go run ./lightweight/cmd/agentflow stdio`
Expected: MCP server starts and accepts `initialize` / `tools/list`.

- [ ] **Step 2: Probe health mode**

Run: `cd D:/myprogram/temporal && GOPROXY=https://goproxy.cn,direct GONOSUMCHECK=* go run ./lightweight/cmd/agentflow`
Expected: HTTP health server starts and prints engine/server assembly logs.

- [ ] **Step 3: Commit**

```bash
git add lightweight/cmd/agentflow lightweight/pkg/engine lightweight/pkg/server
git commit -m "test(lightweight): verify B/C runtime and sync hooks"
```

---

## Self-check coverage

- Engine persistence → Tasks 1-2
- MCP stdio contract → Task 3
- Hub best-effort sync → Task 4
- End-to-end runtime verification → Task 5

## Spec gaps to watch while implementing

- If SQLite persistence becomes too large for `engine.go`, split `store.go` / `history.go` during implementation without changing the plan boundary.
- Keep `agent-hub` changes out of this plan unless Task 4 proves the hub contract is insufficient and a separate repo branch becomes necessary.
