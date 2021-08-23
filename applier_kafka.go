package replicator

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
)

type ApplierKafka struct {
	kafkaWriter *kafka.Writer
}

func (k *ApplierKafka) ApplyRawGtid(ctx context.Context, gtid string) error {
	return nil
}

func (k *ApplierKafka) ApplyGtid() error {
	return nil
}

func (k *ApplierKafka) ApplyBegin() error {
	return nil
}

func (k *ApplierKafka) ApplyCommit() error {
	return nil
}

func (k *ApplierKafka) ApplyField() error {
	return nil
}

func (k *ApplierKafka) ApplyStmt() error {
	return nil
}

func (k *ApplierKafka) ApplyRow() error {
	return nil
}

func (k *ApplierKafka) ApplyOther() error {
	return nil
}

func (k *ApplierKafka) ApplyDDL() error {
	return nil
}

func (k *ApplierKafka) ApplyHeartbeat() error {
	return nil
}
