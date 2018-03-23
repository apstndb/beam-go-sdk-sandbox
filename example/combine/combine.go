package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	output = flag.String("output", "", "Output file (required).")
)
func main() {
	type Emp struct{
		Dept string
		Name string
	}

	ctx := context.Background()
	flag.Parse()
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	emps := beam.CreateList(s, []Emp{
		{"HR", "Alpha"},
		{"Dev", "Charlie"},
		{"Sales", "Bravo"},
		{"Sales", "Delta"},
		{"Dev", "Echo"},
		{"HR", "Foxtrot"},
	})

	groupByDept := beam.ParDo(s, func (e Emp) (string, string) {
		return e.Dept, e.Name
	}, emps)

	namesPerDept := beam.CombinePerKey(s, func(x, y string) string {
		return x + ", " + y
	}, groupByDept)

	textio.Write(s, *output, beam.ParDo(s, func (k, v string) string {
		return k + ": " + v
	}, namesPerDept))

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}

// go run example/sum/sum.go --output=output.txt
