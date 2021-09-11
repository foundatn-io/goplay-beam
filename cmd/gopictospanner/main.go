package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"reflect"

	rdm "github.com/Pallinder/go-randomdata"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"github.com/foundatn-io/goplay-beam/internal/picio"
	"github.com/foundatn-io/goplay-beam/internal/spannerio"
)

const (
	defaultTable = "Word"
)

var (
	defaultConn = &spannerio.Config{
		Project:  "test-project",
		Instance: "test-instance",
		Database: "database",
	}
)

type Word struct {
	Key   int64  `pic:"1,1" spanner:"Id"`
	Value string `pic:"2,10" spanner:"Value"`
}

func (w Word) String() string {
	return fmt.Sprintf("%d%s\n", w.Key, w.Value)
}

func main() {
	ctx := context.Background()

	fn, err := buildDummyFile()
	if err != nil {
		log.Fatalln(ctx, err)
	}

	if err := beamx.Run(ctx, buildNewPICReaderPipeline(ctx, fn)); err != nil {
		log.Fatalln(ctx, err)
	}
}

func buildNewPICReaderPipeline(ctx context.Context, file string) *beam.Pipeline {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	typ := reflect.TypeOf((*Word)(nil)).Elem()

	log.Info(ctx, "pipeline.Add: pic.Extract")
	wordCol := picio.Read(s, file, typ)

	// preferably, would show BQ insert example, but no BQ emulator exists
	log.Info(ctx, "pipeline.Add: spanner.UpsetMutations")
	mutCol := beam.ParDo(s, &spannerio.UpsertMutationFn{
		Table: defaultTable,
	}, wordCol)

	log.Info(ctx, "pipeline.Add: spanner.MutationsCombine")
	mutationSet := beam.Combine(s, &spannerio.MutationSetFn{}, mutCol)

	log.Info(ctx, "pipeline.Add: spanner.MutationsApply")
	beam.ParDo0(s, &spannerio.ApplyMutationFn{Connection: defaultConn.AsConnectionString()}, mutationSet)

	log.Info(ctx, "pipeline.Add: spanner.Read")
	rowCol := spannerio.Read(s, defaultConn.AsConnectionString(), defaultTable, typ)

	log.Info(ctx, "pipeline.Add: rows.Print")
	beam.ParDo0(s, func(w Word) {
		log.Info(ctx, fmt.Sprintf("found element: %+v", w))
	}, rowCol)

	return p
}

func buildDummyFile() (string, error) {
	var words []Word
	for i := 0; i < 10; i++ {
		words = append(words, Word{
			Key:   int64(i),
			Value: rdm.Alphanumeric(9),
		})
	}

	f, err := ioutil.TempFile("", "dummypicfile-*")
	if err != nil {
		return "", err
	}

	defer f.Close()
	for _, w := range words {
		if _, err := f.WriteString(w.String()); err != nil {
			return "", err
		}
	}

	return f.Name(), nil
}
