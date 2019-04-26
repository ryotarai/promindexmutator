# promindexmutator

promindexmutator mutates Prometheus TSDB index. Currently, it only mutates labels including metric names.

## Usage

It can be used as Go library. For example, the following code does:

1. Read `index` index file
2. Prepend `mutated_` prefix to label values
3. Write `index.out` index file

```go
package main

import (
	"log"

	"github.com/ryotarai/promindexmutator"
)

func main() {
	r := promindexmutator.New(promindexmutator.Options{
		LabelsMutator: mutateLabels,
	})
	if err := r.Run("index", "index.out"); err != nil {
		log.Fatal(err)
	}
}

func mutateLabels(lbls promindexmutator.LabelsMap) []promindexmutator.LabelsMap {
	newLbls := promindexmutator.LabelsMap{}
	for k, v := range lbls {
		newLbls[k] = "mutated_" + v
	}
	return []promindexmutator.LabelsMap{newLbls}
}
```

Please make sure no tombstone exists and save backup of the index file.
If any tombstones exist, use `/api/v1/admin/tsdb/clean_tombstones` API to clean them up.

It is recommended to stop Prometheus before mutating index.

## Example

[example/node_exporter_16_labels](https://github.com/ryotarai/promindexmutator/tree/master/example/node_exporter_16_labels) converts v0.15 to v0.16 labels of [node_exporter](https://github.com/prometheus/node_exporter).
