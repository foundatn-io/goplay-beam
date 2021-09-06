package spannerio

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/go/pkg/beam"
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
