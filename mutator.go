package promindexmutator

import (
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

type Mutator struct {
	Options
}

type LabelsMutator func(lbls labels.Labels) []labels.Labels

type Options struct {
	LabelsMutator LabelsMutator
}

type series struct {
	ref  uint64
	lbls labels.Labels
	chks []chunks.Meta
}

func New(options Options) *Mutator {
	if options.LabelsMutator == nil {
		options.LabelsMutator = func(lbls labels.Labels) []labels.Labels {
			return []labels.Labels{lbls}
		}
	}

	return &Mutator{Options: options}
}

func (r *Mutator) Run(input, output string) error {
	reader, err := index.NewFileReader(input)
	if err != nil {
		return errors.Wrap(err, "index.NewFileReader")
	}
	defer func() {
		if err := reader.Close(); err != nil {
			fmt.Printf("warning: error in closing reader: %s\n", err)
		}
	}()

	writer, err := index.NewWriter(output)
	if err != nil {
		return errors.Wrap(err, "index.NewWriter")
	}
	defer func() {
		if err := writer.Close(); err != nil {
			fmt.Printf("warning: error in closing writer: %s\n", err)
		}
	}()

	fromPostings, err := reader.Postings(index.AllPostingsKey())
	if err != nil {
		return errors.Wrap(err, "reader.Postings")
	}

	symbols := map[string]struct{}{}

	type labeledChunks struct {
		lbls labels.Labels
		chks []chunks.Meta
	}
	allChunks := []labeledChunks{}

	for fromPostings.Next() {
		id := fromPostings.At()
		var origLbls labels.Labels
		var chks []chunks.Meta
		if err := reader.Series(id, &origLbls, &chks); err != nil {
			return errors.Wrap(err, "reader.Series")
		}

		for _, lbls := range r.LabelsMutator(origLbls) {
			allChunks = append(allChunks, labeledChunks{lbls: lbls, chks: chks})

			for _, lbl := range lbls {
				symbols[lbl.Name] = struct{}{}
				symbols[lbl.Value] = struct{}{}
			}
		}
	}

	if err := writer.AddSymbols(symbols); err != nil {
		return errors.Wrap(err, "writer.AddSymbols")
	}

	var dupLabels labels.Labels
	sort.Slice(allChunks, func(i, j int) bool {
		v := labels.Compare(allChunks[i].lbls, allChunks[j].lbls)
		if v == 0 {
			dupLabels = allChunks[i].lbls
		}
		if v > 0 {
			return false
		} else {
			return true
		}
	})

	if dupLabels != nil {
		return fmt.Errorf("duplicated labels are not allowed: %+v", dupLabels)
	}

	toPostings := index.NewMemPostings()
	for i, c := range allChunks {
		id := uint64(i)
		if err := writer.AddSeries(id, c.lbls, c.chks...); err != nil {
			return errors.Wrap(err, "writer.AddSeries")
		}
		toPostings.Add(id, c.lbls)
	}

	var name string
	values := []string{}
	for _, l := range toPostings.SortedKeys() {
		if l.Name == "" && l.Value == "" {
			continue
		}
		if name == "" { // first time
			name = l.Name
		}
		if l.Name != name && len(values) > 0 {
			writer.WriteLabelIndex([]string{name}, values)
			name = l.Name
			values = []string{}
		}
		values = append(values, l.Value)
	}
	if len(values) > 0 {
		writer.WriteLabelIndex([]string{name}, values)
	}

	for _, l := range toPostings.SortedKeys() {
		err := writer.WritePostings(l.Name, l.Value, toPostings.Get(l.Name, l.Value))
		if err != nil {
			return errors.Wrap(err, "writer.WritePostings")
		}
	}

	return nil
}
