package main

import (
	"flag"
	"log"
	"strings"

	"github.com/ryotarai/promindexmutator"
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

func mutateLabels(lbls promindexmutator.LabelsMap) []promindexmutator.LabelsMap {
	origName := lbls["__name__"]

	if v, ok := nameTable[origName]; ok {
		lbls["__name__"] = v
	}

	switch origName {
	case "node_cpu":
		lbls["cpu"] = strings.TrimPrefix(lbls["cpu"], "cpu")
	case "node_nfs_procedures":
		lbls["proto"] = lbls["version"]
		lbls["method"] = lbls["procedure"]

		delete(lbls, "version")
		delete(lbls, "procedure")
	}

	return []promindexmutator.LabelsMap{lbls}
}
