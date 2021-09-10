package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"cloud.google.com/go/pubsub"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/pubsubx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/foundatn-io/goplay-beam/internal/spannerio"
)

const (
	defaultProject      = "test-project"
	defaultTopic        = "test-topic"
	defaultSubscription = "test-sub"

	googleCredsKey = "GOOGLE_APPLICATION_CREDENTIALS"

	wordTable = "Word"
)

var (
	defaultConn = &spannerio.Config{
		Project:  "test-project",
		Instance: "test-instance",
		Database: "database",
	}

	cannedMessages = []*pubsub.Message{
		{Data: ([]byte)(`{"id":7, "val":"rst"}`)},
		{Data: ([]byte)(`{"id":8, "val":"uvx"}`)},
		{Data: ([]byte)(`{"id":1, "val":"abc"}`)},
		{Data: ([]byte)(`{"id":2, "val":"def"}`)},
	}
)

type Word struct {
	ID  int64  `spanner:"Id" json:"id"`
	Val string `spanner:"Value" json:"val"`
}

type PubSubConfig struct {
	Project      string
	Topic        string
	Subscription string
}

// reads events from pubsub topic
// marshals into spanner objects
// writes to spanner, reads results back
// prints spanner rows to output
//
// FIXME: does not work. beam/pubsubio only works on dataflow, not direct runner
// ref: https://the-asf.slack.com/archives/CBD9LNKGR/p1628607306018700s
func main() {
	ctx := context.Background()
	beam.Init()
	os.Unsetenv(googleCredsKey)

	// build pipeline
	pipeline := buildStreamingInsertPipeline(ctx, PubSubConfig{
		Project:      defaultProject,
		Topic:        defaultTopic,
		Subscription: defaultSubscription,
	})

	// insert events
	log.Info(ctx, "building pub/sub client")
	ps, err := pubsub.NewClient(ctx, defaultProject)
	if err != nil {
		log.Fatalln(ctx, err)
	}

	log.Info(ctx, "building pub/sub topic")
	t, err := pubsubx.EnsureTopic(ctx, ps, defaultTopic)
	if err != nil {
		log.Fatalln(ctx, err)
	}

	defer pubsubx.CleanupTopic(ctx, defaultProject, defaultTopic)
	log.Info(ctx, "building pub/sub subscription")
	if _, err = pubsubx.EnsureSubscription(ctx, ps, defaultTopic, defaultSubscription); err != nil {
		log.Fatalln(ctx, err)
	}

	for _, msg := range cannedMessages {
		log.Info(ctx, "publishing dummy messages to pub/sub topic", msg)
		_, err := t.Publish(ctx, msg).Get(ctx)
		if err != nil {
			log.Fatalln(ctx, err)
		}
	}

	// execute pipeline
	log.Info(ctx, "executing pipeline...")
	if err := beamx.Run(ctx, pipeline); err != nil {
		log.Fatalln(ctx, err)
	}
}

func buildStreamingInsertPipeline(ctx context.Context, cfg PubSubConfig) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	log.Info(ctx, "pipeline.Add: pubsub.Subscribe")
	msgCol := pubsubio.Read(s, cfg.Project, cfg.Topic, &pubsubio.ReadOptions{Subscription: cfg.Subscription})

	log.Info(ctx, "pipeline.Add: message.Transform")
	wordCol := beam.ParDo(s.Scope("pubsub.MessageTransform"), func(msg []byte) *Word {
		log.Info(ctx, "pubsubio.Message: unmarshalling message into Word")
		w := &Word{}
		if err := json.Unmarshal(msg, w); err != nil {
			log.Fatalln(ctx, err)
		}

		return w
	}, msgCol)

	// preferably, would show BQ insert example, but no BQ emulator exists
	log.Info(ctx, "pipeline.Add: spanner.UpsetMutations")
	mutCol := beam.ParDo(s.Scope("spannerio.Upsert"), &spannerio.UpsertMutationFn{
		Table: wordTable,
	}, wordCol)

	log.Info(ctx, "pipeline.Add: spanner.MutationsCombine")
	mutationSet := beam.Combine(s.Scope("spannerio.CollectMutations"), &spannerio.MutationSetFn{}, mutCol)

	log.Info(ctx, "pipeline.Add: spanner.MutationsApply")
	beam.ParDo0(s.Scope("spannerio.ApplyMutations"), &spannerio.ApplyMutationFn{Connection: defaultConn.AsConnectionString()}, mutationSet)

	log.Info(ctx, "pipeline.Add: spanner.Read")
	wordType := reflect.TypeOf((*Word)(nil)).Elem()
	rowCol := spannerio.Read(s, defaultConn.AsConnectionString(), wordTable, wordType)

	log.Info(ctx, "pipeline.Add: rows.Print")
	beam.ParDo0(s, func(w Word) {
		log.Info(ctx, fmt.Sprintf("found word: %+v", w))
	}, rowCol)

	return p
}
