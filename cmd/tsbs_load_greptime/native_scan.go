package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

type nativeBatch struct {
	table   *table.Table
	rows    uint
	metrics uint64
}

func (b *nativeBatch) Len() uint {
	return b.rows
}

func (b *nativeBatch) Append(item data.LoadedPoint) {
	if b.table == nil {
		that := item.Data.([]byte)
		thatStr := string(that)
		// Each influx line is format "csv-tags csv-fields timestamp", so we split by space
		// and then on the middle element, we split by comma to count number of fields added
		args := strings.Split(thatStr, " ")
		if len(args) != 3 {
			fatal(errNotThreeTuplesFmt, len(args))
			return
		}

		var table_and_tags = strings.Split(args[0], ",")
		b.table, _ = table.New(table_and_tags[0])
		for _, tag := range table_and_tags[1:] {
			b.table.AddTagColumn(strings.Split(tag, "=")[0], types.STRING)
		}

		for _, field := range strings.Split(args[1], ",") {
			b.table.AddFieldColumn(strings.Split(field, "=")[0], types.INT64)
		}

		b.table.AddTimestampColumn("ts", types.TIMESTAMP_NANOSECOND)
	}

	that := item.Data.([]byte)
	thatStr := string(that)git 
	b.rows++
	// Each influx line is format "csv-tags csv-fields timestamp", so we split by space
	// and then on the middle element, we split by comma to count number of fields added
	args := strings.Split(thatStr, " ")
	if len(args) != 3 {
		fatal(errNotThreeTuplesFmt, len(args))
		return
	}
	b.metrics += uint64(len(strings.Split(args[1], ",")))

	var columns []interface{}
	for _, tag := range strings.Split(args[0], ",")[1:] {
		columns = append(columns, strings.Split(tag, "=")[1])
	}

	for _, field := range strings.Split(args[1], ",") {
		var val = strings.Split(field, "=")[1]
		value, err := strconv.ParseInt(val[:len(val)-1], 10, 64)
		if err != nil {
			panic(err)
		}
		columns = append(columns, value)
	}

	ts, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		panic(err)
	}
	columns = append(columns, ts)

	if err := b.table.AddRow(columns...); err != nil {
		fmt.Printf("err: %v\n", err)
	}
}

type nativeFactory struct{}

func (f *nativeFactory) New() targets.Batch {

	return &nativeBatch{table: nil, rows: 0, metrics: 0}
}
