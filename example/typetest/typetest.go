package main

import (
	"flag"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/textio/gcs"
	"strconv"
)

func main() {
	flag.Parse()
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	stringList := beam.CreateList(s, []string{"a", "b", "c"})
	fmt.Println("stringList:", stringList.Type()) // stringList: string

	intList := beam.CreateList(s, []int{1, 2, 3})
	fmt.Println("intList:", intList.Type()) // intList: int

	convList := beam.ParDo(s, strconv.Itoa, intList)
	fmt.Println("convList:", convList.Type()) // convList: string

	convList2 := beam.ParDo(s, strconv.Itoa, stringList)
	fmt.Println("convList2:", convList2.Type()) // panic
}

