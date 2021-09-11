package picio

import (
	"bufio"
	"context"
	"errors"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/foundatn-io/go-pic"
)

func Read(s beam.Scope, fn string, t reflect.Type) beam.PCollection {
	s = s.Scope("picio.Read")

	filesystem.ValidateScheme(fn)
	return read(s, beam.Create(s, fn), t)
}

func read(s beam.Scope, col beam.PCollection, t reflect.Type) beam.PCollection {
	return beam.ParDo(s, &readFn{typ: beam.EncodedType{T: t}}, col, beam.TypeDefinition{Var: beam.XType, T: t})
}

type readFn struct {
	typ beam.EncodedType
}

func (r *readFn) ProcessElement(ctx context.Context, fn string, emit func(x beam.X)) error {
	log.Infof(ctx, "reading file: %s", fn)
	fs, err := filesystem.New(ctx, fn)
	if err != nil {
		return err
	}
	defer fs.Close()

	f, err := fs.OpenRead(ctx, fn)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if scanner.Err() != nil {
			if errors.Is(err, io.EOF) {
				break
			}
		}

		log.Infoln(ctx, "unmarshalling file lines")
		val := reflect.New(r.typ.T).Interface()
		if err := pic.Unmarshal(scanner.Bytes(), val); err != nil {
			return err
		}

		emit(reflect.ValueOf(val).Elem().Interface())
	}

	return nil
}
