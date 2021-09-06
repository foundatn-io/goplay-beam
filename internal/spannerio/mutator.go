package spannerio

import (
	"cloud.google.com/go/spanner"
)

type MutationSetFn struct{}

func (fn *MutationSetFn) CreateAccumulator() *MutationSet {
	return NewMutationSet()
}

func (fn *MutationSetFn) AddInput(m *MutationSet, mut *spanner.Mutation) *MutationSet {
	*m = append(*m, mut)
	return m
}

func (fn *MutationSetFn) MergeAccumulators(a, v *MutationSet) *MutationSet {
	*a = append(*a, *v...)
	return a
}

func (fn *MutationSetFn) ExtractOutput(m MutationBuilder) []*spanner.Mutation {
	return m.GetMutations()
}

// MutationBuilder provides a cleaner API for collecting mutations to be
// applied on a spanner database. There is a set of methods which allow the
// collection of mutations of different kinds. A single GetMutations method is
// also provided allowing the user to extract the resulting set of mutations.
type MutationBuilder interface {
	InsertOrUpdateStruct(table string, in interface{}) error
	GetMutations() []*spanner.Mutation
}

// MutationSet is a wrapper around a []*spanner.Mutation which implements the
// MutationBuilder interface.
//
// Package cloud.google.com/go/spanner does not provide a clean API for
// collecting mutations which are to be applied on a database. Likewise, they
// do not provide an interface around their functions, making it hard to test.
// The use of this will assist in gathering mutations to be applied, whilst also
// providing ease for mocking out at test time.
//
// The methods of this struct directly wrap the spanner package level functions
// with the same name.
type MutationSet []*spanner.Mutation

// NewMutationSet returns an empty mutation set.
func NewMutationSet() *MutationSet {
	return &MutationSet{}
}

// NewMutationBuilder returns MutationSet as MutationBuilder interface.
func NewMutationBuilder() MutationBuilder {
	return &MutationSet{}
}

// GetMutations returns the slice of mutations which have been maintained by the
// MutationSet.
func (m *MutationSet) GetMutations() []*spanner.Mutation {
	return *m
}

// InsertOrUpdateStruct adds a Mutation to insert a row into a table, specified
// by a Go struct. If the row already exists, it updates it instead. Any column
// values not explicitly written are preserved.
//
// The in argument must be a struct or a pointer to a struct. Its exported
// fields specify the column names and values. Use a field tag like
// "spanner:name" to provide an alternative column name, or use "spanner:-" to
// ignore the field.
func (m *MutationSet) InsertOrUpdateStruct(table string, in interface{}) error {
	mutations, err := spanner.InsertOrUpdateStruct(table, in)
	if err != nil {
		return err
	}

	*m = append(*m, mutations)
	return nil
}
