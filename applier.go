package replicator

import (
	"context"
)

type Applier interface {
	ApplyRawGtid(context.Context, string) error

	ApplyGtid() error

	ApplyBegin() error

	ApplyCommit() error

	ApplyField() error

	ApplyStmt() error

	ApplyRow() error

	ApplyOther() error

	ApplyDDL() error

	ApplyHeartbeat() error
}
