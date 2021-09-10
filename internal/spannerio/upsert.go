package spannerio

import (
	"context"

	"cloud.google.com/go/spanner"
)

type UpsertMutationFn struct {
	Transform func(interface{}) interface{}
	Table     string
}

func (u *UpsertMutationFn) ProcessElement(_ context.Context, row interface{}, emit func(mutation *spanner.Mutation)) error {
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
