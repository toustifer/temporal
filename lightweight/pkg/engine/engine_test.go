package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngineTaskLifecycle(t *testing.T) {
	t.Parallel()

	e, err := NewEngine(NewEngineConfig{})
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()

	ns, err := e.CreateNamespace(context.Background(), CreateNamespaceRequest{
		ID:   "ns-1",
		Name: "agent-company",
	})
	require.NoError(t, err)
	require.Equal(t, "ns-1", ns.ID)

	task, err := e.CreateTask(context.Background(), CreateTaskRequest{
		NamespaceID:    "ns-1",
		ID:             "T1",
		Title:          "bootstrap",
		AssignedWorker: "worker-ops",
	})
	require.NoError(t, err)
	require.Equal(t, TaskAssigned, task.State)

	task, err = e.TransitionTask(context.Background(), "ns-1", "T1", TransStart, map[string]string{"actor": "leader"})
	require.NoError(t, err)
	require.Equal(t, TaskExecuting, task.State)

	task, err = e.TransitionTask(context.Background(), "ns-1", "T1", TransSubmit, map[string]string{"actor": "worker-ops", "reason": "self-check done"})
	require.NoError(t, err)
	require.Equal(t, TaskReviewPending, task.State)

	task, err = e.TransitionTask(context.Background(), "ns-1", "T1", TransRework, map[string]string{"actor": "reviewer", "reason": "missing test"})
	require.NoError(t, err)
	require.Equal(t, TaskReworkNeeded, task.State)
	require.Equal(t, 1, task.ReviewCycle)

	history, err := e.GetHistory(context.Background(), "ns-1", "T1")
	require.NoError(t, err)
	require.Len(t, history, 4)
	require.Equal(t, string(TransRework), history[3].Transition)
}

func TestEngineRejectsInvalidTransition(t *testing.T) {
	t.Parallel()

	e, err := NewEngine(NewEngineConfig{})
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()

	_, err = e.CreateNamespace(context.Background(), CreateNamespaceRequest{ID: "ns-1", Name: "agent-company"})
	require.NoError(t, err)
	_, err = e.CreateTask(context.Background(), CreateTaskRequest{NamespaceID: "ns-1", ID: "T1", Title: "bootstrap"})
	require.NoError(t, err)

	_, err = e.TransitionTask(context.Background(), "ns-1", "T1", TransPass, nil)
	require.ErrorIs(t, err, ErrInvalidTransition)
}
