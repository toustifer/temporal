# agentflow — Lightweight Temporal Engine for agent-company

## 一、总览

agentflow 是一个介于 agent-company（多 Agent 编排层）和持久化+协作层之间的**本地状态机服务**。它拿掉 Temporal Server 的 gRPC、membership、分片、多 backend 等分布式负担，只保留**定时的中间状态机核心**，对外暴露两个接口：

- **Go Engine API**（`pkg/engine/`）—— 供其他 Go 代码内嵌调用
- **MCP stdio Server**（`pkg/server/`）—— 供 agent-company 的 Leader/Worker 通过 MCP 协议调用

### 架构定位

```
agent-company (Leader/Worker)
    │  MCP calls (stdio)
    ▼
┌─────────────────────────────────┐
│  agentflow                      │
│  ┌───────────────────────────┐  │
│  │  MCP Server (pkg/server/) │  │
│  │  stdio JSON-RPC 2.0       │  │
│  └────────┬──────────────────┘  │
│           │                     │
│  ┌────────▼──────────────────┐  │
│  │  Engine (pkg/engine/)     │  │
│  │  CreateResource           │  │
│  │  TransitionResource       │  │
│  │  GetResource              │  │
│  │  GetHistory               │  │
│  │  SyncToHub (可选)          │  │
│  └────────┬──────────────────┘  │
│           │                     │
│  ┌────────▼──────────────────┐  │
│  │  Temporal Kernel          │  │
│  │  ExecutionStore           │  │
│  │  HSM state machine        │  │
│  │  SQLite (内存/文件)        │  │
│  └───────────────────────────┘  │
└─────────────────────────────────┘
    │  (可选) HTTP sync
    ▼
agent-hub (多人协作)
```

### 核心原则

1. **本地优先**：所有状态先写 SQLite，操作不依赖外部服务
2. **MCP 原生**：对 agent-company 暴露 MCP stdio 接口，而不是 REST/gRPC
3. **Temporal 内核**：复用 Temporal 的 HSM 状态机和 ExecutionStore 持久化
4. **Hub 可选**：agent-hub 只是同步终点，不是运行时依赖
5. **最小状态集**：只暴露 agent-company 需要的 4 个操作，不把整个 Temporal API 搬过来

---

## 二、核心概念映射

agent-company 的 .mycompany/leader.json 状态模型需要映射到 Temporal 的资源模型。

| agent-company 概念 | Temporal 概念 | 说明 |
|---|---|---|
| `dag[]` 中的单个 task | `WorkflowExecution` | 每个 task 是一个 Temporal Workflow |
| task 的 lifecycle 状态 | `StateMachine.State` | `pending / in_progress / completed` 等 |
| task 的状态流转 | `StateMachine.Transition` | `assigned → executing → review_pending → done` |
| task 的变更记录 | `Event History` | Temporal 自动记录所有 transition |
| DAG 整体 | `Namespace` 或 `Workflow` | 一组关联的 task 共享同一 context |
| worker 会话实例 | `workerAgentId` | MCP 层记录的当前 agent 实例 id |
| resume context | `Memo` / `SearchAttributes` | 存在 workflow 上的键值 metadata |

### 资源类型

agentflow 定义自己的资源类型，**不完全照搬 Temporal 的 Workflow/Activity 语义**。因为 agent-company 的场景比微服务编排要轻得多。

```go
type ResourceType string

const (
    ResourceNamespace ResourceType = "namespace"  // 业务域/项目
    ResourceTask      ResourceType = "task"        // 单个 DAG task
)
```

- 一个 `namespace` 对应一次 session，包含该 session 的配置和元数据
- 一个 `task` 对应 leader.json 里的一行，包含 lifecycle、属主、验收标准
- 所有 `task` 共享同一组 event history 搜索能力

---

## 三、Engine 层（pkg/engine/）

### 3.1 职责

- 初始化 Temporal SQLite 持久化和 HSM 注册
- 提供事务性的资源 CRUD 和状态转换
- 自动记录每次转换到 event history
- 暴露最小接口，不暴露 Temporal 的内部复杂度

### 3.2 核心接口

```go
package engine

// Engine is the core state machine engine.
// All methods are transactional.
type Engine struct {
    // unexported: ExecutionStore, Serializer, HSM Registry
}

// ── Namespace operations ──────────────────────────

// CreateNamespace creates a new namespace (business domain / session).
// Returns the namespace ID.
func (e *Engine) CreateNamespace(ctx context.Context, req CreateNamespaceRequest) (*Namespace, error)

// GetNamespace retrieves a namespace by ID.
func (e *Engine) GetNamespace(ctx context.Context, nsID string) (*Namespace, error)

// ListNamespaces lists all namespaces (active sessions).
func (e *Engine) ListNamespaces(ctx context.Context) ([]Namespace, error)

// ── Task operations ───────────────────────────────

// CreateTask creates a new task in the given namespace.
// Equivalent to adding a new entry to leader.json dag[].
func (e *Engine) CreateTask(ctx context.Context, req CreateTaskRequest) (*Task, error)

// TransitionTask advances a task's state machine.
// The transition is validated: invalid transitions are rejected.
// On success, the event is appended to history.
// Parameters: (namespaceID, taskID, transition, metadata)
func (e *Engine) TransitionTask(ctx context.Context, nsID, taskID string, t TaskTransition, meta map[string]string) (*Task, error)

// GetTask retrieves the current state of a task.
func (e *Engine) GetTask(ctx context.Context, nsID, taskID string) (*Task, error)

// ListTasks lists all tasks in a namespace, optionally filtered by state.
func (e *Engine) ListTasks(ctx context.Context, nsID string, filter StateFilter) ([]Task, error)

// GetHistory retrieves the event history for a task.
func (e *Engine) GetHistory(ctx context.Context, nsID, taskID string) ([]Event, error)

// ── Lifecycle ─────────────────────────────────────

// Close cleanly shuts down the engine and its persistence connections.
func (e *Engine) Close() error
```

### 3.3 Task 数据模型

```go
type Task struct {
    ID               string            `json:"id"`     // taskId (e.g. "T1")
    NamespaceID      string            `json:"namespace_id"`
    Title            string            `json:"title"`
    Description      string            `json:"description"`
    State            TaskState         `json:"state"`  // current lifecycle
    AssignedWorker   string            `json:"assigned_worker"`
    AcceptanceCrieria []string          `json:"acceptance_criteria"`
    OutputFiles      []string          `json:"output_files"`
    WorkerAgentID    string            `json:"worker_agent_id,omitempty"`
    ReviewCycle      int               `json:"review_cycle"`

    CreatedAt        time.Time         `json:"created_at"`
    UpdatedAt        time.Time         `json:"updated_at"`
    Metadata         map[string]string `json:"metadata,omitempty"` // extensible
}
```

### 3.4 TaskState 和 Transition

定义 task 的完整生命周期：

```go
type TaskState string

const (
    TaskAssigned      TaskState = "assigned"       // 刚创建，待执行
    TaskExecuting     TaskState = "executing"       // Worker 正在执行
    TaskReviewPending TaskState = "review_pending"  // 已完成，等待审核
    TaskReworkNeeded  TaskState = "rework_needed"   // 审核未过，需返工
    TaskDone          TaskState = "done"             // 审核通过，已完成
    TaskCancelled     TaskState = "cancelled"        // 取消
)

type TaskTransition string

const (
    TransStart     TaskTransition = "start"      // assigned → executing
    TransSubmit    TaskTransition = "submit"     // executing → review_pending
    TransPass      TaskTransition = "pass"       // review_pending → done
    TransRework    TaskTransition = "rework"     // review_pending → rework_needed
    TransReassign  TaskTransition = "reassign"   // rework_needed → assigned
    TransResume    TaskTransition = "resume"     // rework_needed → executing
    TransCancel    TaskTransition = "cancel"     // any → cancelled
)
```

允许的转换仅在 HSM 注册时定义一次，非法转换被引擎拒绝。

### 3.5 Namespace 数据模型

```go
type Namespace struct {
    ID        string    `json:"id"`
    Name      string    `json:"name"`       // goal / project name
    CreatedAt time.Time `json:"created_at"`
    UpdatedAt time.Time `json:"updated_at"`
    Metadata  map[string]string `json:"metadata,omitempty"`
}
```

### 3.6 Event History 模型

每次 transition 自动产生一条事件记录：

```go
type Event struct {
    TaskID      string    `json:"task_id"`
    Transition  string    `json:"transition"`
    FromState   TaskState `json:"from_state"`
    ToState     TaskState `json:"to_state"`
    Timestamp   time.Time `json:"timestamp"`
    Actor       string    `json:"actor,omitempty"`  // who triggered it
    Reason      string    `json:"reason,omitempty"` // human-readable note
    Metadata    map[string]string `json:"metadata,omitempty"`
}
```

### 3.7 NewEngine 工厂函数

```go
// NewEngineConfig configures the engine's persistence.
type NewEngineConfig struct {
    // SQLite persistence mode
    DBPath string // ":memory:" for in-memory, or file path

    // Optional: agent-hub sync (see SyncConfig)
    SyncConfig *SyncConfig
}

// NewEngine creates and initializes a new engine.
// It sets up SQLite (auto-creates schema), registers the HSM task state machine,
// and returns a ready-to-use engine.
func NewEngine(cfg NewEngineConfig) (*Engine, error)
```

### 3.8 Hub 同步配置

```go
type SyncConfig struct {
    HubMCPEndpoint string // stdio command for the hub MCP server
    BusinessCode   string
    WorkerID       string
    SyncInterval   time.Duration // how often to sync, default 30s
}
```

当 `SyncConfig` 为 nil 时，不同步。当非 nil 时，engine 在后台 goroutine 中自动同步状态变更到 agent-hub。

---

## 四、Server 层（pkg/server/）

### 4.1 职责

- 启动 MCP stdio JSON-RPC 服务器
- 将 MCP 工具调用映射到 Engine 方法调用
- 在每次关键操作后尝试同步 agent-hub
- 向 agent-company 暴露紧凑的工具接口

### 4.2 核心边界

server 层只做三件事：

1. **协议适配**：把 MCP 请求参数转换成 engine 请求
2. **结果包装**：把 engine 返回值转成 MCP tool result
3. **best-effort hub sync**：连上就同步，连不上就跳过，不影响主流程

server 层**不**负责：

- 状态机校验
- 任务生命周期设计
- 同步适配器抽象
- 重试 / 退避 / 任务调度

这些都属于 engine 或外部协作层，不属于 server 的职责。

### 4.3 MCP 工具定义

server 层暴露以下 8 个 MCP 工具：

#### Tool 1: `task_create`

在指定 namespace 中创建一个新 task。

```json
{
  "name": "task_create",
  "description": "Create a new task (DAG entry) under a namespace",
  "inputSchema": {
    "type": "object",
    "properties": {
      "namespace_id": {"type": "string"},
      "task_id": {"type": "string"},
      "title": {"type": "string"},
      "description": {"type": "string"},
      "assigned_worker": {"type": "string"},
      "dependencies": {"type": "array", "items": {"type": "string"}},
      "acceptance_criteria": {"type": "array", "items": {"type": "string"}},
      "output_files": {"type": "array", "items": {"type": "string"}},
      "metadata": {"type": "object"}
    },
    "required": ["namespace_id", "task_id", "title", "assigned_worker"]
  }
}
```

#### Tool 2: `task_transition`

推进一个 task 的状态机。非法转换返回错误。

```json
{
  "name": "task_transition",
  "description": "Advance a task's state machine. Returns error if transition is invalid for current state.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "namespace_id": {"type": "string"},
      "task_id": {"type": "string"},
      "transition": {
        "type": "string",
        "enum": ["start", "submit", "pass", "rework", "reassign", "resume", "cancel"]
      },
      "metadata": {
        "type": "object",
        "properties": {
          "reason": {"type": "string"},
          "actor": {"type": "string"}
        }
      }
    },
    "required": ["namespace_id", "task_id", "transition"]
  }
}
```

#### Tool 3: `task_get`

获取 task 的当前状态和所有字段。

#### Tool 4: `task_list`

列出某个 namespace 下的所有 task，可按状态过滤。

#### Tool 5: `task_history`

获取一个 task 的完整转换历史。

#### Tool 6: `namespace_create`

创建一个新的 namespace（对应 agent-company 的一个 session）。

#### Tool 7: `namespace_list`

列出所有 namespace（活跃 session）。

#### Tool 8: `flow_ping`

存活检查。

### 4.4 请求/响应约定

#### 成功响应

所有 MCP tool 都返回统一结构：

```json
{
  "ok": true,
  "data": {}
}
```

`data` 的具体内容由 tool 决定。

#### 错误响应

所有错误都统一为：

```json
{
  "ok": false,
  "error": {
    "code": "namespace_not_found",
    "message": "namespace ns-1 not found",
    "details": {}
  }
}
```

### 4.5 错误模型

server 层只做协议级错误映射，不吞错：

| 错误码 | 含义 | 来源 |
|---|---|---|
| `invalid_request` | 参数缺失 / 类型错误 | MCP 参数校验 |
| `namespace_not_found` | namespace 不存在 | engine |
| `task_not_found` | task 不存在 | engine |
| `duplicate_namespace` | namespace 重复 | engine |
| `duplicate_task` | task 重复 | engine |
| `invalid_transition` | 非法状态转换 | engine |
| `engine_unavailable` | engine 初始化或运行失败 | server |
| `hub_sync_failed` | hub 同步失败但主流程继续 | best-effort sync |

**原则**：
- `engine_*` 错误影响当前 tool 调用结果
- `hub_sync_failed` 不影响 tool 成功与否，只记录日志

### 4.6 Hub best-effort 同步规则

每次下列操作后都尝试同步到 agent-hub：

- `task_create`
- `task_transition`
- `task_get`
- `task_history`

同步策略：

1. 先尝试调用 hub MCP / API
2. 如果 hub 可达，写入对应的 DAG / event 状态
3. 如果 hub 不可达或未登录，静默跳过
4. 不允许 hub 失败阻塞 MCP 返回

### 4.7 启动方式

agentflow 通过 MCP stdio 模式启动：

```json
{
  "mcpServers": {
    "agentflow": {
      "command": "agentflow",
      "type": "stdio",
      "args": ["--db", ":memory:"],
      "env": {
        "AGENTFLOW_NAMESPACE": "default"
      }
    }
  }
}
```

或者文件模式：

```json
{
  "mcpServers": {
    "agentflow": {
      "command": "agentflow",
      "type": "stdio",
      "args": ["--db", ".mycompany/agentflow.db"]
    }
  }
}
```

---

## 五、执行协议：Leader 和 Worker 的标准化工作循环

agentflow 的状态机定义了 task 的生命周期。但**生命周期怎么驱动**（谁在什么时候做什么事）由一份执行协议定义。

### 6.1 Leader 完整循环

```
goal 进入
    │
    ▼
1. Leader 拆解需求 → 生成 DAG 任务清单
2. 调 agentflow.namespace_create 创建 session
3. 对每个 task 调 agentflow.task_create 创建并写入依赖关系
4. 调 agentflow.task_transition(start) → 状态变为 executing
5. 派 Worker（Agent 调用）
6. Worker 回来后，审核：
   ├─ 通过 → task_transition(pass)
   └─ 不通过 → task_transition(rework) → Worker 返工 → 回到 step 6
7. task 变为 done，从 DAG 移除
8. 所有 task 都 done 后 → 触发 Worker 汇总总结经验
9. 写 Leader diary
```

**关键变化：**
- Leader 不再手动读写 leader.json 的状态字段
- Leader 不再自己判断转换是否合法（agentflow 拒绝非法转换）
- 审核通过后才触发经验总结，不通过不总结

### 6.2 Worker 完整循环

这个问题现在的处理是错误的：**Worker 在第一次代码编译通过后就总结经验，但此时还没有经过审查和测试。**

正确的 Worker 循环应该是：

```
task 分配下来
    │
    ▼
1. 读上下文：读 playbook、读 domain experience、读相关代码
2. 分析问题：理解代码结构、根本原因、修改范围
3. 执行：写代码/改代码
4. 自验：编译、跑已有测试、验证 acceptance criteria
5. 提交：调 agentflow.task_transition(submit) → 状态变为 review_pending
    │
    ▼ 等待 Leader/Reviewer 审查
    │
    ├─ 通过 →
    │    调 agentflow.task_transition(pass)
    │
    └─ 不通过 →
         调 agentflow.task_transition(rework) → 重新回到 step 1
          （注意：不是回到 "写代码"，而是回到 "重新分析" — 
           可能问题不是代码错，而是理解错了需求）
         └─ 循环直到 leader 判定 pass
    │
    ▼ 审查通过后
         测试验证（集成测试、端到端、回归）
    │
    ▼ 测试通过后
         Worker 总结：这个 task 从初版到最终版经历了什么、学到了什么
         Worker 更新 domain experience、写 diary
```

### 6.3 经验总结的时间点（重要）

**现在（错误）：**
```
写代码 → 编译通过 → 总结 ← 太早了，返工可能推翻结论
```

**改为（正确）：**
```
写代码 → 审查 → 返工 → 审查通过 → 测试验证通过 → 全链路完成后总结
```

原因：

1. **第一次的理解可能是错的**。返工过程中可能发现真正的 root cause 和第一版完全不同。如果第一次成功后就已经总结了，返工后的真正结论不会进入经验库。

2. **总结应该覆盖"整条链路"**，不是"第一次写代码的心得"。Worker 在最终通过时最能说清楚：第一条路径为什么失败、第二条路径为什么成功、应该避免什么。

3. **总结不是 Worker 自己的事**。Leader 在审查时也有发现——代码规范、设计决策、遗漏的边界情况。所以最终的经验总结应该是：

```
Worker 总结：技术层面——我做了什么、踩了什么坑、学到了什么
Leader 补充：架构层面——这个 task 的设计选择、对整体架构的影响
两者合并后写入 domain experience
```

### 6.4 状态流转与总结的对应关系

```
task 首次 submitting
    └─ review → pass → [等待全 DAG 完成]
    └─ review → rework → task 返工循环 → 最终 pass → [等待全 DAG 完成]

当 Namespace 下所有 task 都 done 后：
    Leader 通知所有涉及 Worker："全链路完成，现在总结"
    每个 Worker 基于全链路经验写 session / experience / diary
    Leader 写 diary
    这个总结才是最终的 team memory
```

### 6.5 agentflow 对此的支持

agentflow 不直接管"总结"逻辑（那是 agent-company 的知识沉淀层），但它提供触发条件：

- `task_list(namespace_id, state_filter=done)` → 当返回列表 === 全量 task 列表时，Leader 知道可以触发总结了
- `task_history(task_id)` → 供 Worker 回顾完整返工链路
- `transition(metadata.reason)` → 记录每次返工的原因，供总结时回顾

---

## 六、agent-company 集成方式

### 6.1 SKILL.md 改动

在 Phase 0（bootstrap）中，新增一步：

1. 如果 `.mcp.json` 中不存在 `agentflow` server，自动写入配置
2. 调用 `flow_ping` 验证连通性
3. 创建初始 namespace，对应当前的 session/goal

### 6.2 Leader prompt 改动

不再需要手动构造 `leader.json` 状态和 `dag[]` 状态机管理。改为：

```
1. 调用 agentflow.namespace_create 创建当前 session 的 namespace
2. 对每个 task，调用 agentflow.task_create 创建后写入 DAG
3. 派发 Worker，setting task_transition(start) 通过 agentflow
4. 等 Worker 回来，审核后调 task_transition(pass | rework)
5. 所有状态查询都走 agentflow.task_get / task_list
```

### 6.3 Worker prompt 改动

Worker 不再需要手动写 session.json 状态持久化。改为：

```
1. 任务开始时，调 agentflow.task_transition(start)
2. 任务完成后，调 agentflow.task_transition(submit)
3. 如果需要继续写 session/experience/diary，通过 agentflow 的任务 metadata 字段记录
```

---

## 七、未定义区域（后续迭代）

以下功能在 v1 spec 中不定义，留待后续：

### 7.1 资源锁

agent-hub 有 `acquire_lock / release_lock`。agentflow v1 不做锁——它只是一个单进程本地服务，不需要分布式锁。多进程场景（多个 Claude 实例同时编辑同一项目）时再引入。

### 7.2 完整的 Temporal Workflow 语义

agentflow 不暴露 Temporal 的 Timer、Signal、Query、Cron、SideEffect 等特性。这些对 agent-company 的 DAG 管理场景过于复杂。

### 7.3 Playbook 管理

playbook 是 agent-company 自己的 `.mycompany/playbook/` 目录的职责，不进 agentflow。

### 7.4 多人实时协作

多人协作不走 agentflow——仍通过 agent-hub 的 `git push` 分布式锁 + hub 事件同步完成。

---

## 八、与现有 Temporal 源码的关系

agentflow 位于 Temporal 源码树的 `lightweight/` 目录中，**直接使用** Temporal 的 Go 模块：

| Temporal 包 | agentflow 如何使用 |
|---|---|
| `common/persistence/sql/sqlplugin/sqlite` | `init()` 注册 SQLite 驱动 |
| `common/persistence/sql` | `sql.NewFactory()` 创建 store |
| `common/persistence/serialization` | 序列化/反序列化 |
| `common/persistence` | `ExecutionStore` 和 `ExecutionManager` 接口 |
| `service/history/hsm` | 状态机框架（Transition, Node, Registry） |
| `common/config` | `config.SQL` 结构体 |
| `common/log` | 日志接口 |
| `common/metrics` | `NoopMetricsHandler` |
| `common/resolver` | `NoopResolver` |

计划在未来，当 agentflow 稳定后，可以移出 Temporal 独立为一个仓库。但在迭代阶段，保持在同一模块内是最快的方式。

---

## 九、技术验证状态

| 项目 | 状态 |
|---|---|
| 编译（Go 1.26.4, Windows） | ✅ 通过 |
| SQLite 内存模式启动 | ✅ 通过 |
| ExecutionStore 创建 | ✅ 通过 |
| HTTP health 端点 | ✅ 通过 |
| HSM 集成 | ❌ 待实现（当前 PoC 不含）|
| Engine API | ❌ 待实现 |
| MCP Server | ❌ 待实现 |
| Hub 同步 | ❌ 待实现 |
| agent-company 集成 | ❌ 待实现 |

---

## 十、附录：状态机转换表

```
当前状态 \ 转换   | start      submit     pass      rework    reassign  resume    cancel
─────────────────┼───────────────────────────────────────────────────────────
assigned         │ executing    ✗         ✗         ✗         ✗         ✗        cancelled
executing        │   ✗       review_pending ✗       ✗         ✗         ✗        cancelled
review_pending   │   ✗          ✗        done    rework_needed ✗       ✗        cancelled
rework_needed    │   ✗          ✗         ✗         ✗       assigned executing  cancelled
done             │   ✗          ✗         ✗         ✗         ✗         ✗          ✗
cancelled        │   ✗          ✗         ✗         ✗         ✗         ✗          ✗
```
