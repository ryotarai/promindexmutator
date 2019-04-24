package main

import (
	"log"
	"os"

	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

func main() {
	reader, err := index.NewFileReader(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			panic(err)
		}
	}()

	_, err = reader.Symbols()
	if err != nil {
		panic(err)
	}
	// log.Printf("symbols: %+v", symbols)

	postings, err := reader.Postings(index.AllPostingsKey())
	if err != nil {
		panic(err)
	}

	for postings.Next() {
		id := postings.At()
		var lbls labels.Labels
		var chks []chunks.Meta
		err := reader.Series(id, &lbls, &chks)
		if err != nil {
			panic(err)
		}
		log.Printf("series: %+v %+v %+v", id, lbls, chks)
	}
	if err := postings.Err(); err != nil {
		panic(err)
	}

	names, err := reader.LabelNames()
	if err != nil {
		panic(err)
	}
	log.Printf("label names: %+v", names)
	for _, n := range names {
		t, err := reader.LabelValues(n)
		if err != nil {
			panic(err)
		}
		for i := 0; i < t.Len(); i++ {
			strs, err := t.At(i)
			if err != nil {
				panic(err)
			}
			log.Printf("name: %+v, values: %+v", n, strs)
		}
	}
	// writer.WriteLabelIndex(names, values)
}
