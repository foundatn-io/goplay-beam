package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	rdm "github.com/Pallinder/go-randomdata"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/google/uuid"

	"github.com/foundatn-io/goplay-beam/internal/spannerio"
)

const (
	defaultTable   = "Word"
	defaultRootDir = "myDir"
	seedCount      = 15
	groupSize      = 6
)

type Word struct {
	ID  int64  `spanner:"Id" json:"id"`
	Val string `spanner:"Value" json:"val"`
}

func main() {
	ctx := context.Background()
	defaultSpannerCfg := &spannerio.Config{
		Project:  "test-project",
		Instance: "test-instance",
		Database: "database",
	}

	if err := initDB(ctx, defaultSpannerCfg); err != nil {
		log.Fatalln(err)
	}

	log.Println("building pipeline...")
	beam.Init()
	p := newGroupRowsByNPipeline(ctx, defaultSpannerCfg)

	log.Println("starting pipeline...")
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalln(err)
	}

	log.Println("inspecting written files...")
	fs, err := filesystem.New(ctx, defaultRootDir)
	files, err := fs.List(ctx, fmt.Sprintf("%s/*", defaultRootDir))
	if err != nil {
		log.Fatalln(err)
	}

	for _, f := range files {
		log.Println(f)
	}
}

func initDB(ctx context.Context, cfg *spannerio.Config) error {
	log.Println("generating spanner seed data...")
	c, err := spannerio.New(ctx, cfg)
	if err != nil {
		return err
	}

	mb := spannerio.NewMutationBuilder()
	for i := 0; i < seedCount; i++ {
		w := &Word{
			ID:  int64(i),
			Val: rdm.SillyName(),
		}

		if err := mb.InsertOrUpdateStruct(defaultTable, w); err != nil {
			return err
		}
	}

	log.Println("seeding spanner data...")
	_, err = c.Apply(ctx, mb.GetMutations())
	return err
}

// Groups spanner row elements by arbitrary size
// for example, group a set of 9 elems by 2, producing 5 groups, with on oddly sized group:
// {uuid3}; [7:rst 8:uvx] | {uuid4}; [9:wyz] | {uuid0}; [1:abc 2:def] | {uuid1}; [3:ghi 4:jhk] | {uuid2}; [5:lmn 6:opq]
// then writes each group to its own file, for example:
// {uuid3}.json (containing {"id":7, "val":"rst"} \n {"id":8, "val":"uvx"})
//
// This example makes use of a spanner emulator to seed and retrieve data, and
// uses the local filesystem implementation to write the JSON files. The
// filesystem implementation can easily be substituted with the GCS counterpart
func newGroupRowsByNPipeline(_ context.Context, cfg *spannerio.Config) *beam.Pipeline {
	pipeline, rootScope := beam.NewPipelineWithRoot()

	log.Println("pipeline.Add: spanner.Query")
	// Select all words from DB
	wordType := reflect.TypeOf((*Word)(nil)).Elem()
	words := spannerio.Read(rootScope, cfg.AsConnectionString(), defaultTable, wordType)

	log.Println("pipeline.Add: collection.AddGroupByNKey")
	// Break word set into groups
	keyedWords := beam.ParDo(rootScope, &groupByFn{counter: 0, size: groupSize, key: uuid.New()}, words)

	log.Println("pipeline.Add: collection.GroupByKey")
	groupedWords := beam.GroupByKey(rootScope, keyedWords)

	log.Println("pipeline.Add: file.Write")
	// write each group of files to file
	beam.ParDo0(rootScope, &writeFn{}, groupedWords)
	return pipeline
}

type groupByFn struct {
	key     uuid.UUID
	counter int
	size    int
}

func (f *groupByFn) ProcessElement(_ context.Context, v Word, emit func(uuid.UUID, Word)) error {
	log.Printf("grouping %d words by key: %s", f.size, f.key)
	emit(f.key, v)
	f.counter++
	if f.counter == f.size {
		f.counter = 0
		u, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		f.key = u
	}

	return nil
}

type writeFn struct{}

func (f *writeFn) ProcessElement(ctx context.Context, k uuid.UUID, vIter func(word *Word) bool) error {
	log.Printf("processing word group: %s", k)
	var w Word
	var words []Word
	var data []byte

	// extract values
	for vIter(&w) {
		log.Printf("adding word: %+v", w)
		words = append(words, w)
	}

	log.Println("marshalling words into json")
	for _, elem := range words {
		b, err := json.Marshal(&elem)
		if err != nil {
			return err
		}

		data = append(data, b...)
		// add new line so files JSON is new-line delimited
		data = append(data, []byte("\n")...)
	}

	if err := f.write(ctx, fmt.Sprintf("%s/%s.json", defaultRootDir, k), data); err != nil {
		return err
	}

	return nil
}

func (f *writeFn) write(ctx context.Context, filename string, data []byte) error {
	log.Println("initialising filesystem....")
	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return err
	}

	log.Println("building file writer")
	wc, err := fs.OpenWrite(ctx, filename)
	if err != nil {
		return err
	}
	defer wc.Close()

	log.Printf("writing file: %s", filename)
	if _, err := wc.Write(data); err != nil {
		return err
	}

	return nil
}
