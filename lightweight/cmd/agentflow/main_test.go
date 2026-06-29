package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	lwserver "go.temporal.io/server/lightweight/pkg/server"
)

// --- existing infrastructure smoke-test -----------------------------------

func TestMainBuildsServer(t *testing.T) {
	t.Parallel()

	components, err := buildComponents()
	require.NoError(t, err)
	t.Cleanup(func() {
		components.close()
	})

	require.NotNil(t, components)
	require.NotNil(t, components.engine)
	require.NotNil(t, components.server)
	require.NotEmpty(t, components.backendName)
	require.NotEmpty(t, components.tools)
	require.Contains(t, toolNames(components.tools), "flow_ping")
}

// --- MCP stdio pipeline tests ---------------------------------------------

func TestServeMCP_FullPipeline(t *testing.T) {
	components, err := buildComponents()
	require.NoError(t, err)
	t.Cleanup(components.close)

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errCh := make(chan error, 1)
	go func() {
		errCh <- serveMCP(ctx, inR, outW, components.server)
	}()

	t.Run("initialize", func(t *testing.T) {
		writeMCP(t, inW, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
		raw := readMCP(t, outR)

		var resp rpcResponse
		require.NoError(t, json.Unmarshal([]byte(raw), &resp))
		require.Equal(t, "2.0", resp.JSONRPC)
		idFloat, ok := resp.ID.(float64)
		require.True(t, ok, "id should be a JSON number")
		require.Equal(t, float64(1), idFloat)
		require.Nil(t, resp.Error)

		result, ok := resp.Result.(map[string]any)
		require.True(t, ok, "result should be an object")
		require.Equal(t, "2024-11-05", result["protocolVersion"])
		_, hasCaps := result["capabilities"]
		require.True(t, hasCaps, "result should include capabilities")
	})

	t.Run("tools/list returns 8 tools", func(t *testing.T) {
		writeMCP(t, inW, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
		raw := readMCP(t, outR)

		var resp rpcResponse
		require.NoError(t, json.Unmarshal([]byte(raw), &resp))
		require.Equal(t, "2.0", resp.JSONRPC)
		require.Nil(t, resp.Error)

		result, ok := resp.Result.(map[string]any)
		require.True(t, ok, "result should be an object")

		toolsRaw, ok := result["tools"]
		require.True(t, ok, "result should contain tools key")

		// Re-marshal to get typed ToolSpec slice.
		toolsJSON, err := json.Marshal(toolsRaw)
		require.NoError(t, err)
		var tools []lwserver.ToolSpec
		require.NoError(t, json.Unmarshal(toolsJSON, &tools))
		require.Len(t, tools, 8, "expected exactly 8 tools")

		names := make([]string, 0, len(tools))
		for _, tool := range tools {
			names = append(names, tool.Name)
		}
		assert.Contains(t, names, "flow_ping")
		assert.Contains(t, names, "namespace_create")
		assert.Contains(t, names, "namespace_list")
		assert.Contains(t, names, "task_create")
		assert.Contains(t, names, "task_transition")
		assert.Contains(t, names, "task_get")
		assert.Contains(t, names, "task_list")
		assert.Contains(t, names, "task_history")
	})

	t.Run("tools/call flow_ping returns MCP content format", func(t *testing.T) {
		writeMCP(t, inW, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"flow_ping","arguments":{}}}`)
		raw := readMCP(t, outR)

		var resp rpcResponse
		require.NoError(t, json.Unmarshal([]byte(raw), &resp))
		require.Nil(t, resp.Error, "flow_ping should not error")

		result, ok := resp.Result.(map[string]any)
		require.True(t, ok, "result should be an object")

		content, ok := result["content"].([]any)
		require.True(t, ok, "content should be an array")
		require.Len(t, content, 1)

		first, ok := content[0].(map[string]any)
		require.True(t, ok, "first content item should be an object")
		require.Equal(t, "text", first["type"])
		text, ok := first["text"].(string)
		require.True(t, ok, "text should be a string")
		require.Contains(t, text, `"ok":true`, "tool result should include ok:true")
	})

	t.Run("tools/call routes to server Handle", func(t *testing.T) {
		// First create a namespace via MCP.
		nsReq := `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"namespace_create","arguments":{"id":"ns-test","name":"test-ns"}}}`
		writeMCP(t, inW, nsReq)
		raw := readMCP(t, outR)
		var nsResp rpcResponse
		require.NoError(t, json.Unmarshal([]byte(raw), &nsResp))
		require.Nil(t, nsResp.Error, "namespace_create should succeed via MCP")

		// Create a task in that namespace via MCP.
		createReq := `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"task_create","arguments":{"namespace_id":"ns-test","task_id":"T-mcp","title":"mcp-created","assigned_worker":"w"}}}`
		writeMCP(t, inW, createReq)
		raw = readMCP(t, outR)
		var createResp rpcResponse
		require.NoError(t, json.Unmarshal([]byte(raw), &createResp))
		require.Nil(t, createResp.Error, "task_create should succeed via MCP")
		require.NotNil(t, createResp.Result)
		content, ok := createResp.Result.(map[string]any)["content"].([]any)
		require.True(t, ok, "content should be an array")
		require.Len(t, content, 1)
		first, ok := content[0].(map[string]any)
		require.True(t, ok, "first content item should be an object")
		require.Equal(t, "text", first["type"])
		text, ok := first["text"].(string)
		require.True(t, ok, "text should be a string")
		require.Contains(t, text, `"id":"T-mcp"`)

		// Verify persistence by reading the task back.
		getReq := `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"task_get","arguments":{"namespace_id":"ns-test","task_id":"T-mcp"}}}`
		writeMCP(t, inW, getReq)
		raw = readMCP(t, outR)
		var getResp rpcResponse
		require.NoError(t, json.Unmarshal([]byte(raw), &getResp))
		require.Nil(t, getResp.Error, "task_get should succeed via MCP")
	})

	t.Run("unknown tool returns MCP error", func(t *testing.T) {
		writeMCP(t, inW, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"does_not_exist","arguments":{}}}`)
		raw := readMCP(t, outR)

		var resp rpcResponse
		require.NoError(t, json.Unmarshal([]byte(raw), &resp))
		require.NotNil(t, resp.Error, "unknown tool should error")
		require.Equal(t, -32603, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "unknown tool")
	})

	t.Run("unknown method returns MCP error", func(t *testing.T) {
		writeMCP(t, inW, `{"jsonrpc":"2.0","id":8,"method":"bogus","params":{}}`)
		raw := readMCP(t, outR)

		var resp rpcResponse
		require.NoError(t, json.Unmarshal([]byte(raw), &resp))
		require.NotNil(t, resp.Error, "bogus method should error")
		require.Equal(t, -32601, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "method not found")
	})

	t.Run("clean shutdown on stdin close", func(t *testing.T) {
		inW.Close()
		select {
		case err := <-errCh:
			require.NoError(t, err, "serveMCP should exit cleanly on EOF")
		case <-time.After(10 * time.Second):
			t.Fatal("serveMCP did not exit within 10 s after stdin closed")
		}
	})
}

func TestServeMCP_MultipleConsecutiveMessages(t *testing.T) {
	components, err := buildComponents()
	require.NoError(t, err)
	t.Cleanup(components.close)

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errCh := make(chan error, 1)
	go func() {
		errCh <- serveMCP(ctx, inR, outW, components.server)
	}()

	// Read responses in a goroutine to prevent pipe deadlock.
	type response struct {
		raw string
		err error
	}
	respCh := make(chan response, 3)
	go func() {
		for i := 0; i < 3; i++ {
			raw, err := readMCPRaw(outR)
			respCh <- response{raw: raw, err: err}
		}
	}()

	// Send three requests back-to-back.
	writeMCP(t, inW, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	writeMCP(t, inW, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	writeMCP(t, inW, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"flow_ping","arguments":{}}}`)

	// Collect responses.
	resps := make([]rpcResponse, 0, 3)
	for i := 0; i < 3; i++ {
		r := <-respCh
		require.NoError(t, r.err)
		var parsed rpcResponse
		require.NoError(t, json.Unmarshal([]byte(r.raw), &parsed))
		resps = append(resps, parsed)
	}

	// Verify all three responses arrived in order.
	require.Len(t, resps, 3)
	require.Nil(t, resps[0].Error)
	id0, _ := resps[0].ID.(float64)
	require.Equal(t, float64(1), id0)
	_, ok := resps[0].Result.(map[string]any)
	require.True(t, ok, "initialize result should be an object")

	require.Nil(t, resps[1].Error)
	id1, _ := resps[1].ID.(float64)
	require.Equal(t, float64(2), id1)
	toolsResult, ok := resps[1].Result.(map[string]any)
	require.True(t, ok)
	tools, ok := toolsResult["tools"]
	require.True(t, ok, "tools/list result should contain tools")
	toolArr, ok := tools.([]any)
	require.True(t, ok)
	require.Len(t, toolArr, 8)

	require.Nil(t, resps[2].Error)
	id2, _ := resps[2].ID.(float64)
	require.Equal(t, float64(3), id2)
	pingResult, ok := resps[2].Result.(map[string]any)
	require.True(t, ok)
	content, ok := pingResult["content"].([]any)
	require.True(t, ok)
	require.Len(t, content, 1)

	inW.Close()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("serveMCP did not exit")
	}
}

// --- MCP stdio helpers ----------------------------------------------------

func writeMCP(t *testing.T, w io.Writer, payload string) {
	t.Helper()
	_, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n%s", len(payload), payload)
	require.NoError(t, err)
}

func readMCP(t *testing.T, r io.Reader) string {
	t.Helper()
	raw, err := readMCPRaw(r)
	require.NoError(t, err)
	return raw
}

// readMCPRaw reads one MCP message without t.Helper, safe for goroutines.
func readMCPRaw(r io.Reader) (string, error) {
	br := bufio.NewReader(r)

	var contentLength int
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimSuffix(line, "\r\n")
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			break
		}
		if strings.HasPrefix(line, "Content-Length:") {
			if _, err := fmt.Sscanf(line, "Content-Length: %d", &contentLength); err != nil {
				return "", fmt.Errorf("parse Content-Length: %w", err)
			}
		}
	}
	if contentLength <= 0 {
		return "", fmt.Errorf("missing Content-Length header")
	}

	body := make([]byte, contentLength)
	if _, err := io.ReadFull(br, body); err != nil {
		return "", err
	}
	return string(body), nil
}
