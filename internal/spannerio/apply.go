package spannerio

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

func Apply(s beam.Scope, conn string, mutations []*spanner.Mutation) {
	s = s.Scope("spanner.Apply")
	apply(s, conn, mutations)
}

func apply(s beam.Scope, conn string, mutations []*spanner.Mutation) {
	imp := beam.Impulse(s)
	beam.ParDo0(s, &applyFn{Connection: conn, Mutations: mutations}, imp)
}

type applyFn struct {
	Connection string
	Mutations  []*spanner.Mutation
}

func (f *applyFn) ProcessElement(ctx context.Context, _ []byte) error {
	client, err := spanner.NewClient(ctx, f.Connection)
	if err != nil {
		return err
	}

	_, err = client.Apply(ctx, f.Mutations)
	if err != nil {
		return err
	}

	return nil
}

type ApplyMutationFn struct {
	Connection string
}

func (f *ApplyMutationFn) ProcessElement(ctx context.Context, mutations []*spanner.Mutation) error {
	log.Info(ctx, "spannerio.Apply: building spanner client")
	client, err := spanner.NewClient(ctx, f.Connection)
	if err != nil {
		return err
	}

	log.Info(ctx, "spannerio.Apply: applying mutations")
	_, err = client.Apply(ctx, mutations)
	if err != nil {
		return err
	}

	return nil
}
