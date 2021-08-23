package replicator

import (
	"context"
)

type ApplierMysql struct {
	dbClient *DBClient
}

func (m *ApplierMysql) ApplyRawGtid(ctx context.Context, gtid string) error {
	// FIXME currently shutdown setNextGtid because of requiring super privilege
	if false && !m.dbClient.InTransaction {
		return m.dbClient.SetNextGtid(ctx, gtid)
	}
	return nil
}

func (m *ApplierMysql) ApplyGtid() error {
	return nil
}

func (m *ApplierMysql) ApplyBegin() error {
	return nil
}

func (m *ApplierMysql) ApplyCommit() error {
	return nil
}

func (m *ApplierMysql) ApplyField() error {
	return nil
}

func (m *ApplierMysql) ApplyStmt() error {
	return nil
}

func (m *ApplierMysql) ApplyRow() error {
	return nil
}

func (m *ApplierMysql) ApplyOther() error {
	return nil
}

func (m *ApplierMysql) ApplyDDL() error {
	return nil
}

func (m *ApplierMysql) ApplyHeartbeat() error {
	return nil
}
