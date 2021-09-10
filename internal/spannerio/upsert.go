package spannerio

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

type UpsertMutationFn struct {
	Transform func(interface{}) interface{}
	Table     string
}

func (u *UpsertMutationFn) ProcessElement(ctx context.Context, row interface{}, emit func(mutation *spanner.Mutation)) error {
	log.Info(ctx, "spannerio.Mutations: building InsertOrUpdateStruct mutation")
	obj := row
	if u.Transform != nil {
		obj = u.Transform(row)
	}

	m, err := spanner.InsertOrUpdateStruct(u.Table, obj)
	if err != nil {
		return err
	}

	emit(m)
	return nil
}
