/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"context"

	"github.com/nicholaskh/opendts/dbconnpool"
	"github.com/nicholaskh/opendts/proto"
	"github.com/nicholaskh/opendts/sqltypes"
)

// MysqlDaemon is the interface we use for abstracting Mysqld.
type MysqlDaemon interface {
	// Schema related methods
	GetSchema(ctx context.Context, dbName string, tables, excludeTables []string, includeViews bool) (*proto.SchemaDefinition, error)
	GetColumns(ctx context.Context, dbName, table string) ([]*proto.Field, []string, error)
	GetPrimaryKeyColumns(ctx context.Context, dbName, table string) ([]string, error)

	// GetDbaConnection returns a dba connection.
	GetDbaConnection(ctx context.Context) (*dbconnpool.DBConnection, error)

	// FetchSuperQuery executes one query, returns the result
	FetchSuperQuery(ctx context.Context, query string) (*sqltypes.Result, error)

	// Close will close this instance of Mysqld. It will wait for all dba
	// queries to be finished.
	Close()
}
