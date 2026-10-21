package connpubsub

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// A failed publish RPC used to deadlock SyncRecords. Under load the pipeline is
// full: the consumer waits on a batch, the pool is blocked sending its next
// result, and the main loop is blocked in pool.Run. When the batch then fails,
// the consumer stops reading after one result, which frees one slot: the pool
// sends one more message result and blocks on that record's LSN sentinel for
// good, and the main loop never gets back to the select that would see the
// cancelled batch. The activity kept heartbeating, so the mirror sat in
// STATUS_RUNNING until someone paused it.
//
// Runs against the Pub/Sub emulator (PUBSUB_EMULATOR_HOST). The first batch is
// held back long enough for the pipeline to fill, and the topic is deleted
// underneath it, so the whole batch fails as a real outage would.
func TestSyncRecordsFailsInsteadOfHangingOnPublishError(t *testing.T) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("PUBSUB_EMULATOR_HOST not set")
	}
	ctx := t.Context()

	// Publishers copy these defaults when SyncRecords creates them.
	defaults := pubsub.DefaultPublishSettings
	t.Cleanup(func() { pubsub.DefaultPublishSettings = defaults })
	pubsub.DefaultPublishSettings.DelayThreshold = 2 * time.Second
	pubsub.DefaultPublishSettings.CountThreshold = 1000

	client, err := pubsub.NewClient(ctx, "peerdb-test")
	require.NoError(t, err)
	defer client.Close()

	topic := "deadlock_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	topicName := "projects/peerdb-test/topics/" + topic
	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
	require.NoError(t, err)

	// No catalog behind this connector: every setting is pinned through Env, the
	// flush loop never fires, and the failure path never reaches FinishBatch.
	c := &PubSubConnector{client: client, logger: internal.LoggerFromCtx(ctx)}
	const buffered = 1024
	stream := model.NewCDCStream[model.RecordItems](buffered)
	var consumed atomic.Int64
	req := &model.SyncRecordsRequest[model.RecordItems]{
		Records:        stream,
		ConsumedOffset: &consumed,
		FlowJobName:    "pubsub_deadlock_test",
		TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      "public." + topic,
			DestinationTableIdentifier: topic,
		}},
		Env: map[string]string{
			"PEERDB_QUEUE_PARALLELISM":           "1",
			"PEERDB_QUEUE_FLUSH_TIMEOUT_SECONDS": "3600",
			"PEERDB_QUEUE_FORCE_TOPIC_CREATION":  "false",
		},
		SyncBatchID: 1,
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.SyncRecords(ctx, req)
		done <- err
	}()

	feedCtx, stopFeeding := context.WithCancel(ctx)
	defer stopFeeding()
	go func() {
		for i := int64(1); feedCtx.Err() == nil; i++ {
			items := model.NewRecordItems(1)
			items.AddColumn("payload", types.QValueString{Val: "x"})
			record := &model.InsertRecord[model.RecordItems]{
				Items:                items,
				SourceTableName:      "public." + topic,
				DestinationTableName: topic,
				BaseRecord: model.BaseRecord{
					CheckpointID:   i,
					CommitTimeNano: time.Now().UnixNano(),
				},
			}
			if err := stream.AddRecord(feedCtx, record); err != nil {
				return
			}
		}
	}()

	// The stream only backs up once the main loop is blocked behind the full
	// pipeline, which is the state the hang needs.
	require.Eventually(t, func() bool { return stream.ChannelLen() == buffered }, 10*time.Second, 10*time.Millisecond)
	err = client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topicName})
	require.NoError(t, err)

	select {
	case err := <-done:
		require.ErrorContains(t, err, "error publishing message")
	case <-time.After(30 * time.Second):
		t.Fatal("SyncRecords did not return after a failed publish")
	}
}
