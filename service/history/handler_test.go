package history

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestDescribeHistoryHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	controller := shard.NewMockController(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	h := Handler{
		config: &configs.Config{
			NumberOfShards: 10,
		},
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewNoopLogger(),
		controller:        controller,
		namespaceRegistry: namespaceRegistry,
		hostInfoProvider:  hostInfoProvider,
	}

	mockShard1 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(1)).Return(mockShard1, serviceerror.NewShardOwnershipLost("", ""))

	_, err := h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 1,
	})
	assert.Error(t, err)
	var sol *serviceerror.ShardOwnershipLost
	assert.True(t, errors.As(err, &sol))

	mockShard2 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 2,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(2)).Return(mockShard2, nil)
	controller.EXPECT().ShardIDs().Return([]int32{2})
	namespaceRegistry.EXPECT().GetRegistrySize().Return(int64(0), int64(0))
	hostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("0.0.0.0"))
	_, err = h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 2,
	})
	assert.NoError(t, err)
}

func TestScheduledEventIDFromStateMachineRef(t *testing.T) {
	t.Run("recovers id from last key", func(t *testing.T) {
		ref := &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{Type: "nexusoperations.Operation", Id: "42"},
			},
		}
		id, err := scheduledEventIDFromStateMachineRef(ref)
		require.NoError(t, err)
		require.Equal(t, int64(42), id)
	})
	t.Run("uses the last (deepest) key", func(t *testing.T) {
		ref := &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{Type: "nexusoperations.Operation", Id: "7"},
				{Type: "nexusoperations.Cancelation", Id: "99"},
			},
		}
		id, err := scheduledEventIDFromStateMachineRef(ref)
		require.NoError(t, err)
		require.Equal(t, int64(99), id)
	})
	t.Run("empty path is an error", func(t *testing.T) {
		_, err := scheduledEventIDFromStateMachineRef(&persistencespb.StateMachineRef{})
		require.Error(t, err)
	})
	t.Run("non-numeric id is an error", func(t *testing.T) {
		ref := &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{Type: "x", Id: "not-a-number"}},
		}
		_, err := scheduledEventIDFromStateMachineRef(ref)
		require.Error(t, err)
	})
}

func TestChasmNexusCompletionFromHSMRequest(t *testing.T) {
	t.Run("success carries the payload", func(t *testing.T) {
		payload := &commonpb.Payload{Data: []byte("ok")}
		req := &historyservice.CompleteNexusOperationRequest{
			State:          string(nexus.OperationStateSucceeded),
			OperationToken: "op-token",
			Completion:     &tokenspb.NexusOperationCompletion{RequestId: "req-1"},
			Outcome:        &historyservice.CompleteNexusOperationRequest_Success{Success: payload},
		}
		completion, err := chasmNexusCompletionFromHSMRequest(req)
		require.NoError(t, err)
		require.Equal(t, "req-1", completion.GetRequestId())
		require.Equal(t, "op-token", completion.GetOperationToken())
		require.NotNil(t, completion.GetSuccess())
		require.Equal(t, []byte("ok"), completion.GetSuccess().GetData())
	})
	t.Run("failure is converted to a temporal failure", func(t *testing.T) {
		req := &historyservice.CompleteNexusOperationRequest{
			State:      string(nexus.OperationStateFailed),
			Completion: &tokenspb.NexusOperationCompletion{RequestId: "req-2"},
			Outcome: &historyservice.CompleteNexusOperationRequest_Failure{
				Failure: &nexuspb.Failure{Message: "boom"},
			},
		}
		completion, err := chasmNexusCompletionFromHSMRequest(req)
		require.NoError(t, err)
		require.NotNil(t, completion.GetFailure())
		require.Equal(t, "boom", completion.GetFailure().GetMessage())
	})
}

func TestScheduledEventIDFromComponentPath(t *testing.T) {
	t.Run("recovers id from Operations path", func(t *testing.T) {
		id, err := scheduledEventIDFromComponentPath([]string{"Operations", "42"})
		require.NoError(t, err)
		require.Equal(t, int64(42), id)
	})
	t.Run("uses the last segment", func(t *testing.T) {
		id, err := scheduledEventIDFromComponentPath([]string{"Operations", "7", "13"})
		require.NoError(t, err)
		require.Equal(t, int64(13), id)
	})
	t.Run("empty path is an error", func(t *testing.T) {
		_, err := scheduledEventIDFromComponentPath(nil)
		require.Error(t, err)
	})
	t.Run("non-numeric segment is an error", func(t *testing.T) {
		_, err := scheduledEventIDFromComponentPath([]string{"Operations", "nope"})
		require.Error(t, err)
	})
}

func TestNexusOperationErrorFromChasmRequest(t *testing.T) {
	t.Run("success yields no operation error", func(t *testing.T) {
		req := &historyservice.CompleteNexusOperationChasmRequest{
			Completion: &tokenspb.NexusOperationCompletion{RequestId: "r"},
			Outcome:    &historyservice.CompleteNexusOperationChasmRequest_Success{Success: &commonpb.Payload{}},
		}
		opErr, err := nexusOperationErrorFromChasmRequest(req)
		require.NoError(t, err)
		require.Nil(t, opErr)
	})
	t.Run("failure yields an operation error", func(t *testing.T) {
		req := &historyservice.CompleteNexusOperationChasmRequest{
			Completion: &tokenspb.NexusOperationCompletion{RequestId: "r"},
			Outcome: &historyservice.CompleteNexusOperationChasmRequest_Failure{
				Failure: &failurepb.Failure{Message: "boom"},
			},
		}
		opErr, err := nexusOperationErrorFromChasmRequest(req)
		require.NoError(t, err)
		require.NotNil(t, opErr)
	})
}
