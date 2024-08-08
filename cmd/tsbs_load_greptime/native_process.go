package main

import (
	"context"
	"strconv"
	"strings"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/timescale/tsbs/pkg/targets"
)

type nativeProcessor struct {
	client *greptime.Client
}

func (p *nativeProcessor) Init(numWorker int, _, _ bool) {
	daemonURL := daemonURLs[numWorker%len(daemonURLs)]
	var endpoint = strings.Split(daemonURL, ":")
	port, err := strconv.Atoi(endpoint[1])
	if err != nil {
		panic(err)
	}
	// fmt.Printf("host: %s, port: %s, db: %s\n", endpoint[0], endpoint[1], loader.DatabaseName())
	cfg := greptime.NewConfig(endpoint[0]).WithPort(port).WithDatabase(loader.DatabaseName())
	client, err := greptime.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	p.client = client
}

func (p *nativeProcessor) Close(_ bool) {
	p.client.CloseStream(context.Background())
}

func (p *nativeProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*nativeBatch)

	// fmt.Printf("Trying to write %d rows, %d metrics\n", batch.rows, batch.metrics)
	// Write the batch: try until backoff is not needed.
	if doLoad {
		// var err error
		// fmt.Printf("Writing %d rows, %d metrics\n", batch.rows, batch.metrics)
		err := p.client.StreamWrite(context.Background(), batch.table)
		// err = p.client.StreamWrite(context.Background(), batch.table)
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
		// fmt.Printf("affected rows: %d\n", resp.GetAffectedRows().GetValue())
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows
	return metricCnt, uint64(rowCnt)
}
