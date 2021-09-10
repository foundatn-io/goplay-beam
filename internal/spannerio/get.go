package spannerio

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"google.golang.org/api/iterator"
)

// Read reads all rows from the given table. The table must have a schema
// compatible with the given type, t, and Read returns a PCollection<t>. If the
// table has more rows than t, then Read is implicitly a projection.
func Read(s beam.Scope, conn, table string, t reflect.Type) beam.PCollection {
	s = s.Scope("spanner.Read")
	return query(s, conn, spanner.NewStatement(fmt.Sprintf("SELECT * from %v", table)), t)
}

// Query executes a query. The output must have a schema compatible with the given
// type, t. It returns a PCollection<t>.
func Query(s beam.Scope, conn string, q spanner.Statement, t reflect.Type) beam.PCollection {
	s = s.Scope("spanner.Query")
	return query(s, conn, q, t)
}

func query(s beam.Scope, conn string, stmt spanner.Statement, t reflect.Type) beam.PCollection {
	imp := beam.Impulse(s)
	return beam.ParDo(s, &queryFn{Connection: conn, Query: stmt, Type: beam.EncodedType{T: t}}, imp, beam.TypeDefinition{Var: beam.XType, T: t})
}

type queryFn struct {
	Connection string            `json:"connection"`
	Query      spanner.Statement `json:"query"`
	Type       beam.EncodedType  `json:"type"`
}

func (f *queryFn) ProcessElement(ctx context.Context, _ []byte, emit func(beam.X)) error {
	client, err := spanner.NewClient(ctx, f.Connection)
	if err != nil {
		return err
	}

	defer client.Close()
	it := client.ReadOnlyTransaction().Query(ctx, f.Query)
	for {
		val := reflect.New(f.Type.T).Interface() // val : *T
		r, err := it.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}

			return err
		}

		err = r.ToStruct(val)
		if err != nil {
			return err
		}

		emit(reflect.ValueOf(val).Elem().Interface()) // emit(*val)
	}

	return nil
}
