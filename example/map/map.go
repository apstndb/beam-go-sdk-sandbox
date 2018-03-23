package main

import (
	"context"
	"flag"
	"strconv"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

var (
	output = flag.String("output", "", "Output file (required).")
)

func main() {
	ctx := context.Background()
	flag.Parse()
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	var input beam.PCollection = beam.Create(s, 1, 2, 3, 4)

	var square beam.PCollection = beam.ParDo(s, func(x int) int {
		return x * x
	}, input) // int to int

	var strings beam.PCollection = beam.ParDo(s, strconv.Itoa, square)

	debug.Print(s, strings)
	textio.Write(s, *output, strings)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
