package promindexmutator

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"github.com/stretchr/testify/assert"
)

var baset = int64(1556165184000)

func createSampleTSDB(dir string) (string, error) {
	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	st, err := tsdb.Open(dir, l, nil, &tsdb.Options{
		RetentionDuration: 15 * 24 * 60 * 60 * 1000,
		BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
	})
	if err != nil {
		return "", err
	}
	defer st.Close()

	appender := st.Appender()
	appender.Add(labels.Labels{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
	}, baset, 1234.0)
	if err := appender.Commit(); err != nil {
		return "", err
	}

	snapshotDir := filepath.Join(dir, "snapshot")
	if err := st.Snapshot(snapshotDir, true); err != nil {
		return "", err
	}

	return snapshotDir, nil
}

func TestMutatorRun(t *testing.T) {
	dir, err := ioutil.TempDir("", "promindexmutator-test")
	if !assert.NoError(t, err) {
		return
	}
	defer os.RemoveAll(dir)

	snapshotDir, err := createSampleTSDB(dir)
	if !assert.NoError(t, err) {
		return
	}

	indexPaths, err := filepath.Glob(filepath.Join(snapshotDir, "*", "index"))
	if !assert.NoError(t, err) {
		return
	}

	if !assert.Equal(t, 1, len(indexPaths)) {
		return
	}
	indexPath := indexPaths[0]

	m := New(Options{
		LabelsMutator: func(lbls LabelsMap) []LabelsMap {
			newLbls := LabelsMap{}
			for k, v := range lbls {
				newLbls[k] = "mutated_" + v
			}
			return []LabelsMap{lbls, newLbls}
		},
	})
	outputPath := indexPath + ".out"

	err = m.Run(indexPath, outputPath)
	if !assert.NoError(t, err) {
		return
	}

	err = os.Rename(outputPath, indexPath)
	if !assert.NoError(t, err) {
		return
	}

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	st, err := tsdb.Open(snapshotDir, l, nil, &tsdb.Options{
		RetentionDuration: 15 * 24 * 60 * 60 * 1000,
		BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
	})
	if !assert.NoError(t, err) {
		return
	}

	q, err := st.Querier(baset-1, baset)
	if !assert.NoError(t, err) {
		return
	}
	defer q.Close()

	set, err := q.Select(labels.NewEqualMatcher("__name__", "mutated_foo"))
	if !assert.NoError(t, err) {
		return
	}
	if !assert.True(t, set.Next()) {
		return
	}
	s := set.At()
	assert.Equal(t, "mutated_baz", s.Labels().Map()["bar"])

	it := s.Iterator()
	it.Next()
	tt, v := it.At()
	assert.Equal(t, baset, tt)
	assert.Equal(t, float64(1234), v)
}
