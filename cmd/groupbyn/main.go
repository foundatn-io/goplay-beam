package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/google/uuid"
)

func main() {
	ctx := context.Background()
	beam.Init()
	if err := beamx.Run(ctx, newGroupPipeline(ctx)); err != nil {
		log.Fatalln(err)
	}
}

// Groups elems by arbitrary size
// for example, group a set of 9 elems by 2, producing 5 groups, with on oddly sized group:
// {uuid3}; [rst uvx] | {uuid4}; [wyz] | {uuid0}; [abc def] | {uuid1}; [ghi jhk] | {uuid2}; [lmn opq]
// then writes each group to its own file, for example:
// {uuid3}.txt (containing rst \n uvx)
func newGroupPipeline(_ context.Context) *beam.Pipeline {
	pipeline, rootScope := beam.NewPipelineWithRoot()
	words := []string{"abc", "def", "ghi", "jhk", "lmn", "opq", "rst", "uvx", "wyz"}
	wordCol := beam.CreateList(rootScope, words)
	wordKeys := beam.ParDo(rootScope, &groupCountFn{counter: 0, size: 2}, wordCol)
	wordGroups := beam.GroupByKey(rootScope, wordKeys)
	beam.ParDo0(rootScope, &writeKVFn{Scope: rootScope}, wordGroups)
	return pipeline
}

type groupCountFn struct {
	key     uuid.UUID
	counter int
	size    int
}

func (f *groupCountFn) ProcessElement(_ context.Context, word string, emit func(uuid.UUID, string)) {
	emit(f.key, word)
	f.counter++
	if f.counter == f.size {
		f.counter = 0
		f.key = uuid.New()
	}
}

type writeKVFn struct {
	Scope beam.Scope
}

func (f *writeKVFn) ProcessElement(ctx context.Context, key uuid.UUID, wordIter func(*string) bool) {
	wc, err := local.New(ctx).OpenWrite(ctx, fmt.Sprintf("%s.txt", key))
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := wc.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	var s string
	for wordIter(&s) {
		if _, err := wc.Write([]byte(fmt.Sprintf("%s\n", s))); err != nil {
			log.Fatalln(err)
		}
	}
}
