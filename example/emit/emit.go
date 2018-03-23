package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

func main() {
	ctx := context.Background()
	flag.Parse()
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	input := beam.Create(s, 1, 2, 3, 4)

	double := beam.ParDo(s, func(x int, emit func(int)) {
		emit(x)
		emit(x)
	}, input) // int to multiple int

	debug.Print(s, double)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
