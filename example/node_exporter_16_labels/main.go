package main

import (
	"flag"
	"log"
	"strings"

	"github.com/ryotarai/promindexmutator"

	"github.com/prometheus/tsdb/labels"
)

func main() {
	inputFile := flag.String("input", "", "input index file")
	outputFile := flag.String("output", "", "output index file")
	flag.Parse()

	if *inputFile == "" {
		log.Fatal("-input must be provided")
	}
	if *outputFile == "" {
		log.Fatal("-output must be provided")
	}

	r := promindexmutator.New(promindexmutator.Options{
		LabelsMutator: mutateLabels,
	})
	if err := r.Run(*inputFile, *outputFile); err != nil {
		log.Fatal(err)
	}

	log.Println("done")
}

func mutateLabels(lbls labels.Labels) []labels.Labels {
	var origName string
	for i, l := range lbls {
		if l.Name == "__name__" {
			origName = l.Value
			if v, ok := nameTable[origName]; ok {
				l.Value = v
				lbls[i] = l
			}
		}
	}

	switch origName {
	case "node_cpu":
		for i, l := range lbls {
			if l.Name == "cpu" {
				l.Value = strings.TrimPrefix(l.Value, "cpu")
				lbls[i] = l
			}
		}
	case "node_nfs_procedures":
		for i, l := range lbls {
			switch l.Name {
			case "version":
				l.Name = "proto"
				lbls[i] = l
			case "procedure":
				l.Name = "method"
				lbls[i] = l
			}
		}
	}

	return []labels.Labels{lbls}
}
