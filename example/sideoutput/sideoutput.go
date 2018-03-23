package main

import (
	"context"
	"flag"
	"strconv"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	oddFile = flag.String("oddFile", "", "Output file (required).")

	evenFile = flag.String("evenFile", "", "Output file (required).")
)

func main() {
	ctx := context.Background()
	flag.Parse()
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	ints := beam.ParDo(s, func(_ []byte, emit func(int)) {
		for i := 0; i < 100; i++ {
			emit(i)
		}
	}, beam.Impulse(s)) // generate PCollection in runtime

	evens, odds := beam.ParDo2(s, func(x int, emitEven func(int), emitOdd func(int)) {
		if x % 2 == 0 {
			emitEven(x)
		} else {
			emitOdd(x)
		}
	}, ints) // side output

	textio.Write(s.Scope("Write Odds"), *oddFile, beam.ParDo(s, strconv.Itoa, odds))
	textio.Write(s.Scope("Write Evens"), *evenFile, beam.ParDo(s, strconv.Itoa, evens))

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}

// go run example/sideoutput/sideoutput.go --oddFile=odd.txt --evenFile=even.txt --runner=dataflow --project=apstndb-sandbox --staging_location=gs://apstndb-sandbox-bucket/staging
