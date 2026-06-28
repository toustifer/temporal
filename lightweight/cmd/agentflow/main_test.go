package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
