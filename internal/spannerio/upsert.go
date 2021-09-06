package spannerio

import (
	"context"

	"cloud.google.com/go/spanner"
)

type UpsertMutationFn struct {
	Transform func(interface{}) interface{}
	Table     string
}

func (u *UpsertMutationFn) ProcessElement(ctx context.Context, row interface{}, emit func(mutation *spanner.Mutation)) error {
	m, err := spanner.InsertOrUpdateStruct(u.Table, u.Transform(row))
	if err != nil {
		return err
	}

	emit(m)
	return nil
}
